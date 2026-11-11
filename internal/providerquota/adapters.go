package providerquota

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/qwen"
)

const quotaPersistenceTimeout = 5 * time.Second

type Observer interface {
	ObserveProviderCall(model, outcome, errorClass string, duration time.Duration)
	ObserveProviderTokens(model, source string, tokens int64)
	ObserveProviderTokenReservation(model string, tokens int64)
	ObserveProviderLedgerPersistenceFailure(model, operation string)
	ObserveQuotaAdmission(model, result string)
	ObserveQuotaWait(model string, duration time.Duration)
}

type EmbeddingProvider interface {
	EmbedDocuments(context.Context, []string) (embedding.Result, error)
	EmbedQuery(context.Context, string) (embedding.Result, error)
}

type EmbeddingAdapter struct {
	provider EmbeddingProvider
	limiter  *Limiter
	observer Observer
}

func NewEmbeddingAdapter(provider EmbeddingProvider, limiter *Limiter, observers ...Observer) (*EmbeddingAdapter, error) {
	if provider == nil || limiter == nil || limiter.model != embedding.Model {
		return nil, errors.New("embedding quota adapter requires the matching provider and limiter")
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	if limiter.observer == nil {
		limiter.observer = observer
	}
	return &EmbeddingAdapter{provider: provider, limiter: limiter, observer: observer}, nil
}

func (a *EmbeddingAdapter) EmbedDocuments(ctx context.Context, texts []string) (embedding.Result, error) {
	return a.call(ctx, texts, func(callCtx context.Context) (embedding.Result, error) {
		return a.provider.EmbedDocuments(callCtx, texts)
	})
}

func (a *EmbeddingAdapter) EmbedQuery(ctx context.Context, query string) (embedding.Result, error) {
	return a.call(ctx, []string{query}, func(callCtx context.Context) (embedding.Result, error) {
		return a.provider.EmbedQuery(callCtx, query)
	})
}

func (a *EmbeddingAdapter) call(ctx context.Context, texts []string,
	invoke func(context.Context) (embedding.Result, error)) (embedding.Result, error) {
	var estimate int64 = 128
	for _, text := range texts {
		if !utf8.ValidString(text) {
			return embedding.Result{}, errors.New("embedding quota estimate received invalid UTF-8")
		}
		estimate += int64(len(text))
	}
	reservationID, err := a.limiter.Reserve(ctx, estimate)
	if err != nil {
		return embedding.Result{}, err
	}
	started := time.Now()
	result, callErr := invoke(ctx)
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaPersistenceTimeout)
	defer cancel()
	if callErr != nil {
		if a.observer != nil {
			a.observer.ObserveProviderCall(embedding.Model, "failure", providerErrorClass(callErr), time.Since(started))
		}
		code, requestID, retryAfter := embeddingFailure(callErr)
		if finishErr := a.limiter.Fail(finishCtx, reservationID, requestID, code, retryAfter); finishErr != nil {
			if a.observer != nil {
				a.observer.ObserveProviderLedgerPersistenceFailure(embedding.Model, "fail")
			}
			return embedding.Result{}, errors.Join(callErr, finishErr)
		}
		return embedding.Result{}, callErr
	}
	actualTokens := result.UsageTokens
	tokenSource := "supplier_actual"
	if actualTokens == 0 {
		actualTokens = estimate
		tokenSource = "fallback_estimate"
	}
	if a.observer != nil {
		a.observer.ObserveProviderCall(embedding.Model, "success", "none", time.Since(started))
		a.observer.ObserveProviderTokens(embedding.Model, tokenSource, actualTokens)
	}
	if err := a.limiter.Complete(finishCtx, reservationID, actualTokens, result.RequestID); err != nil {
		if a.observer != nil {
			a.observer.ObserveProviderLedgerPersistenceFailure(embedding.Model, "complete")
		}
		return embedding.Result{}, fmt.Errorf("persist embedding provider usage: %w", err)
	}
	return result, nil
}

type QwenAdapter struct {
	provider *qwen.Client
	limiter  *Limiter
	observer Observer
}

func NewQwenAdapter(provider *qwen.Client, limiter *Limiter, observers ...Observer) (*QwenAdapter, error) {
	if provider == nil || limiter == nil || limiter.model != qwen.Model {
		return nil, errors.New("Qwen quota adapter requires the matching provider and limiter")
	}
	var observer Observer
	if len(observers) > 0 {
		observer = observers[0]
	}
	if limiter.observer == nil {
		limiter.observer = observer
	}
	return &QwenAdapter{provider: provider, limiter: limiter, observer: observer}, nil
}

func (a *QwenAdapter) CompleteAnswer(ctx context.Context, systemPrompt, userPrompt string) (qwen.Result, error) {
	messages := []qwen.Message{{Role: "system", Content: systemPrompt}, {Role: "user", Content: userPrompt}}
	return a.call(ctx, messages, 2048, 8192, func(callCtx context.Context) (qwen.Result, error) {
		return a.provider.CompleteAnswer(callCtx, systemPrompt, userPrompt)
	})
}

func (a *QwenAdapter) CompleteDecision(ctx context.Context, messages []qwen.Message) (qwen.Result, error) {
	return a.call(ctx, messages, 768, 8192, func(callCtx context.Context) (qwen.Result, error) {
		return a.provider.CompleteDecision(callCtx, messages)
	})
}

func (a *QwenAdapter) CompleteStructured(ctx context.Context, messages []qwen.Message, maxTokens int) (qwen.Result, error) {
	return a.call(ctx, messages, maxTokens, 8192, func(callCtx context.Context) (qwen.Result, error) {
		return a.provider.CompleteStructured(callCtx, messages, maxTokens)
	})
}

func (a *QwenAdapter) CompleteStructuredSchema(ctx context.Context, messages []qwen.Message,
	schemaName string, schema map[string]any, maxTokens int) (qwen.Result, error) {
	extra, err := json.Marshal(struct {
		Name   string         `json:"name"`
		Schema map[string]any `json:"schema"`
	}{schemaName, schema})
	if err != nil {
		return qwen.Result{}, fmt.Errorf("estimate Qwen schema usage: %w", err)
	}
	return a.call(ctx, messages, maxTokens, int64(len(extra)), func(callCtx context.Context) (qwen.Result, error) {
		return a.provider.CompleteStructuredSchema(callCtx, messages, schemaName, schema, maxTokens)
	})
}

func (a *QwenAdapter) call(ctx context.Context, messages []qwen.Message, maxTokens int, extraBytes int64,
	invoke func(context.Context) (qwen.Result, error)) (qwen.Result, error) {
	if maxTokens < 1 || maxTokens > 8192 || extraBytes < 0 {
		return qwen.Result{}, errors.New("Qwen quota estimate is invalid")
	}
	encoded, err := json.Marshal(messages)
	if err != nil {
		return qwen.Result{}, fmt.Errorf("estimate Qwen input usage: %w", err)
	}
	// Until a tokenizer matching the hosted model is available, count each UTF-8
	// input byte plus the maximum output and schema/tool allowance as a conservative
	// admission estimate; provider usage replaces it after a successful response.
	estimate := int64(len(encoded)) + int64(maxTokens) + extraBytes + 128
	reservationID, err := a.limiter.Reserve(ctx, estimate)
	if err != nil {
		return qwen.Result{}, err
	}
	started := time.Now()
	result, callErr := invoke(ctx)
	finishCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), quotaPersistenceTimeout)
	defer cancel()
	if callErr != nil {
		if a.observer != nil {
			a.observer.ObserveProviderCall(qwen.Model, "failure", providerErrorClass(callErr), time.Since(started))
		}
		code, requestID, retryAfter := qwenFailure(callErr)
		if finishErr := a.limiter.Fail(finishCtx, reservationID, requestID, code, retryAfter); finishErr != nil {
			if a.observer != nil {
				a.observer.ObserveProviderLedgerPersistenceFailure(qwen.Model, "fail")
			}
			return qwen.Result{}, errors.Join(callErr, finishErr)
		}
		return qwen.Result{}, callErr
	}
	actual := result.PromptTokens + result.CompletionTokens
	if actual < 0 {
		return qwen.Result{}, errors.New("Qwen provider returned invalid token usage")
	}
	tokenSource := "supplier_actual"
	if actual == 0 {
		actual = estimate
		tokenSource = "fallback_estimate"
	}
	if a.observer != nil {
		a.observer.ObserveProviderCall(qwen.Model, "success", "none", time.Since(started))
		a.observer.ObserveProviderTokens(qwen.Model, tokenSource, actual)
	}
	if err := a.limiter.Complete(finishCtx, reservationID, actual, result.RequestID); err != nil {
		if a.observer != nil {
			a.observer.ObserveProviderLedgerPersistenceFailure(qwen.Model, "complete")
		}
		return qwen.Result{}, fmt.Errorf("persist Qwen provider usage: %w", err)
	}
	return result, nil
}

func providerErrorClass(err error) string {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return "canceled"
	}
	var embeddingErr *embedding.APIError
	if errors.As(err, &embeddingErr) {
		return statusErrorClass(embeddingErr.StatusCode, embeddingErr.Code)
	}
	var qwenErr *qwen.APIError
	if errors.As(err, &qwenErr) {
		return statusErrorClass(qwenErr.StatusCode, qwenErr.Code)
	}
	return "provider_error"
}

func statusErrorClass(status int, code string) string {
	if status == 429 {
		return "rate_limited"
	}
	if status == 0 && strings.EqualFold(code, "TRANSPORT_ERROR") {
		return "transport_error"
	}
	if status >= 500 {
		return "server_error"
	}
	if status >= 400 {
		return "client_error"
	}
	return "provider_error"
}

func embeddingFailure(err error) (code, requestID string, retryAfter time.Duration) {
	var apiErr *embedding.APIError
	if errors.As(err, &apiErr) {
		code, requestID, retryAfter = apiErr.Code, apiErr.RequestID, apiErr.RetryAfter
		if apiErr.StatusCode == 429 && retryAfter <= 0 {
			retryAfter = 2 * time.Second
		}
	}
	if code == "" {
		code = "PROVIDER_CALL_FAILED"
	}
	return code, requestID, retryAfter
}

func qwenFailure(err error) (code, requestID string, retryAfter time.Duration) {
	var apiErr *qwen.APIError
	if errors.As(err, &apiErr) {
		code, requestID, retryAfter = apiErr.Code, apiErr.RequestID, apiErr.RetryAfter
		if apiErr.StatusCode == 429 && retryAfter <= 0 {
			retryAfter = 2 * time.Second
		}
	}
	if code == "" {
		code = "PROVIDER_CALL_FAILED"
	} else {
		code = strings.ToUpper(code)
	}
	return code, requestID, retryAfter
}
