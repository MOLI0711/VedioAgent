package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	Model          = "text-embedding-v4"
	Dimension      = 1024
	MaxBatch       = 10
	MaxResponse    = 1 << 20
	requestTimeout = 30 * time.Second
)

type APIError struct {
	StatusCode int
	Code       string
	Message    string
	RequestID  string
	RetryAfter time.Duration
	Retryable  bool
}

func (e *APIError) Error() string {
	if e.Message != "" {
		return fmt.Sprintf("Bailian Embedding HTTP %d (%s): %s", e.StatusCode, e.Code, e.Message)
	}
	return fmt.Sprintf("Bailian Embedding HTTP %d (%s)", e.StatusCode, e.Code)
}

type Item struct {
	Index     int
	Embedding []float64
}

type Result struct {
	RequestID   string
	UsageTokens int64
	Items       []Item
}

type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

func New(workspaceID, apiKey string) (*Client, error) {
	if workspaceID == "" || apiKey == "" || len(workspaceID) > 128 || len(apiKey) > 512 {
		return nil, errors.New("Embedding provider requires a valid workspace ID and API key")
	}
	for _, r := range workspaceID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-') {
			return nil, errors.New("Embedding workspace ID contains invalid characters")
		}
	}
	endpoint := "https://" + workspaceID + ".cn-beijing.maas.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding"
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("Embedding endpoint is invalid")
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          32,
		MaxIdleConnsPerHost:   8,
		MaxConnsPerHost:       16,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 25 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	httpClient := &http.Client{Transport: transport, Timeout: requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{endpoint: endpoint, apiKey: apiKey, http: httpClient}, nil
}

type requestBody struct {
	Model      string `json:"model"`
	Input      input  `json:"input"`
	Parameters struct {
		Dimension  int    `json:"dimension"`
		TextType   string `json:"text_type"`
		OutputType string `json:"output_type"`
	} `json:"parameters"`
}

type input struct {
	Texts []string `json:"texts"`
}

type responseBody struct {
	RequestID string `json:"request_id"`
	Code      string `json:"code"`
	Message   string `json:"message"`
	Output    struct {
		Embeddings []struct {
			Index     int       `json:"index"`
			Embedding []float64 `json:"embedding"`
		} `json:"embeddings"`
	} `json:"output"`
	Usage struct {
		TotalTokens int64 `json:"total_tokens"`
	} `json:"usage"`
}

func (c *Client) EmbedDocuments(ctx context.Context, texts []string) (Result, error) {
	return c.embed(ctx, texts, "document")
}

func (c *Client) EmbedQuery(ctx context.Context, query string) (Result, error) {
	return c.embed(ctx, []string{query}, "query")
}

func (c *Client) embed(ctx context.Context, texts []string, textType string) (Result, error) {
	if c == nil || c.http == nil || c.endpoint == "" || c.apiKey == "" {
		return Result{}, errors.New("Embedding client is not initialized")
	}
	if textType != "document" && textType != "query" {
		return Result{}, errors.New("Embedding text type must be document or query")
	}
	if len(texts) == 0 || len(texts) > MaxBatch {
		return Result{}, fmt.Errorf("Embedding batch size must be between 1 and %d", MaxBatch)
	}
	for i, text := range texts {
		if text == "" || !utf8.ValidString(text) {
			return Result{}, fmt.Errorf("Embedding input %d is empty or invalid UTF-8", i)
		}
	}
	body := requestBody{Model: Model, Input: input{Texts: append([]string(nil), texts...)}}
	body.Parameters.Dimension = Dimension
	body.Parameters.TextType = textType
	body.Parameters.OutputType = "dense"
	encoded, err := json.Marshal(body)
	if err != nil {
		return Result{}, fmt.Errorf("encode Embedding request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return Result{}, fmt.Errorf("create Embedding request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return Result{}, &APIError{Code: "TRANSPORT_ERROR", Message: "Embedding request transport failed", Retryable: true}
	}
	defer response.Body.Close()
	responseBytes, err := readBounded(response.Body, MaxResponse)
	if err != nil {
		return Result{}, err
	}
	var decoded responseBody
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		_ = json.Unmarshal(responseBytes, &decoded)
		apiErr := &APIError{StatusCode: response.StatusCode, Code: decoded.Code,
			Message: boundedMessage(decoded.Message), RequestID: decoded.RequestID}
		if response.StatusCode == http.StatusTooManyRequests {
			apiErr.Retryable = true
			apiErr.RetryAfter = retryAfter(response.Header.Get("Retry-After"))
		} else if response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500 {
			apiErr.Retryable = true
		}
		if apiErr.Code == "" {
			apiErr.Code = http.StatusText(response.StatusCode)
		}
		if apiErr.Message == "" {
			apiErr.Message = "Embedding provider returned a non-success response"
		}
		return Result{}, apiErr
	}
	if err := json.Unmarshal(responseBytes, &decoded); err != nil {
		return Result{}, fmt.Errorf("decode Embedding response: %w", err)
	}
	if decoded.Code != "" || decoded.Output.Embeddings == nil || decoded.Usage.TotalTokens < 0 {
		return Result{}, errors.New("Embedding success response is missing output or contains an error")
	}
	items := make([]Item, len(decoded.Output.Embeddings))
	for i, raw := range decoded.Output.Embeddings {
		items[i] = Item{Index: raw.Index, Embedding: raw.Embedding}
	}
	return Result{RequestID: decoded.RequestID, UsageTokens: decoded.Usage.TotalTokens, Items: items}, nil
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read Embedding response: %w", err)
	}
	if int64(len(body)) > limit {
		return nil, errors.New("Embedding response exceeds the size limit")
	}
	return body, nil
}

func retryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		if seconds > 900 {
			seconds = 900
		}
		return time.Duration(seconds) * time.Second
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return min(delay, 15*time.Minute)
		}
	}
	return 0
}

func boundedMessage(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 384 {
		return value[:384]
	}
	return value
}

func ValidateVectors(result Result, expectedCount int) ([][]float32, error) {
	if expectedCount <= 0 || len(result.Items) != expectedCount {
		return nil, errors.New("Embedding response item count does not match request")
	}
	vectors := make([][]float32, expectedCount)
	seen := make([]bool, expectedCount)
	for _, item := range result.Items {
		if item.Index < 0 || item.Index >= expectedCount || seen[item.Index] {
			return nil, errors.New("Embedding response contains duplicate or out-of-range input indices")
		}
		if len(item.Embedding) != Dimension {
			return nil, fmt.Errorf("Embedding input %d returned %d dimensions; expected %d", item.Index, len(item.Embedding), Dimension)
		}
		vector := make([]float32, Dimension)
		var norm float64
		for i, value := range item.Embedding {
			if math.IsNaN(value) || math.IsInf(value, 0) || value > math.MaxFloat32 || value < -math.MaxFloat32 {
				return nil, fmt.Errorf("Embedding input %d contains an invalid numeric value", item.Index)
			}
			vector[i] = float32(value)
			norm += value * value
		}
		if norm == 0 || math.IsInf(norm, 0) || math.IsNaN(norm) {
			return nil, fmt.Errorf("Embedding input %d returned a zero or invalid vector", item.Index)
		}
		vectors[item.Index] = vector
		seen[item.Index] = true
	}
	for index, exists := range seen {
		if !exists {
			return nil, fmt.Errorf("Embedding response omitted input index %d", index)
		}
	}
	return vectors, nil
}
