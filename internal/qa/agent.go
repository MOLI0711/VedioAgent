package qa

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/qwen"
	"mirrorcore/internal/repository/postgres"
)

type agentState struct {
	messages               []qwen.Message
	evidence               []postgres.QuestionEvidence
	byEvidenceID           map[string]postgres.QuestionEvidence
	bySentence             map[int]string
	bytesUsed              int
	truncated              bool
	logicalCalls           int
	httpAttempts           int
	retries                int
	toolAttempts           int
	queryEmbeddingAttempts int
	nextSeq                int
	seenActions            map[string]struct{}
}

func newAgentState(question postgres.Question, evidence []postgres.QuestionEvidence, truncated bool,
	embeddingAttempts int, strategy string) (*agentState, error) {
	var system, user string
	var err error
	switch strategy {
	case StrategyAgent:
		system, user, err = buildPrompts(question, evidence)
	case StrategyFixedRAG:
		system, user, err = buildFixedRAGPrompts(question, evidence)
	default:
		return nil, &RunError{Code: "QA_STRATEGY_INVALID", Message: "问答生成策略不受支持"}
	}
	if err != nil {
		return nil, err
	}
	state := &agentState{messages: []qwen.Message{{Role: "system", Content: system}, {Role: "user", Content: user}},
		evidence: append([]postgres.QuestionEvidence(nil), evidence...), byEvidenceID: make(map[string]postgres.QuestionEvidence, len(evidence)),
		bySentence: make(map[int]string, len(evidence)), truncated: truncated, nextSeq: 2,
		queryEmbeddingAttempts: embeddingAttempts,
		seenActions:            make(map[string]struct{})}
	for _, item := range evidence {
		state.byEvidenceID[item.ID] = item
		state.bySentence[item.SentenceSeq] = item.ID
		state.bytesUsed += len(item.Text)
	}
	return state, nil
}

func (s *Service) runAgent(ctx context.Context, question postgres.Question, state *agentState, progress ProgressFunc) (qwen.Result, error) {
	if s.generator == nil {
		return qwen.Result{}, &RunError{Code: "GENERATION_PROVIDER_NOT_CONFIGURED", Message: "问答生成服务未配置", Retryable: true}
	}
	for round := 1; round <= 2 && state.logicalCalls < maxLogicalCalls-1; round++ {
		if err := ctx.Err(); err != nil {
			return qwen.Result{}, err
		}
		if time.Until(question.DeadlineAt) <= finalizeReserve {
			break
		}
		if progress != nil {
			if err := progress("agent_deciding"); err != nil {
				return qwen.Result{}, err
			}
		}
		seq := state.nextSeq
		state.nextSeq++
		inputHash, err := hashMessages(state.messages)
		if err != nil {
			return qwen.Result{}, err
		}
		if err := s.repository.BeginQuestionStep(ctx, question, seq, "GENERATION", inputHash); err != nil {
			return qwen.Result{}, err
		}
		beforeHTTP := state.httpAttempts
		completion, callErr := s.callModel(ctx, question, state, finalizeReserve, func(callCtx context.Context) (qwen.Result, error) {
			return s.generator.CompleteDecision(callCtx, state.messages)
		})
		state.logicalCalls++
		outcomeCode := "SUCCEEDED"
		metadata := map[string]any{"phase": "decision", "logical_round": round, "http_attempts": state.httpAttempts - beforeHTTP}
		if callErr != nil {
			outcomeCode = providerOutcome(callErr)
			encoded, _ := json.Marshal(metadata)
			_ = s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, providerRequestID(callErr),
				0, 0, 0, outcomeCode, encoded)
			return qwen.Result{}, providerRunError("AGENT_DECISION_FAILED", "Agent决策调用失败", callErr)
		}
		metadata["tool_calls"] = len(completion.ToolCalls)
		encodedMetadata, _ := json.Marshal(metadata)
		if err := s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, completion.RequestID,
			completion.PromptTokens, completion.CompletionTokens, 0, completion.FinishReason, encodedMetadata); err != nil {
			return qwen.Result{}, err
		}
		state.messages = append(state.messages, qwen.Message{Role: "assistant", Content: completion.Content,
			ToolCalls: completion.ToolCalls})
		if len(completion.ToolCalls) == 0 {
			if completion.FinishReason != "stop" {
				return qwen.Result{}, &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "Agent决策响应未正常结束"}
			}
			break
		}
		if completion.FinishReason != "tool_calls" {
			return qwen.Result{}, &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "工具调用响应缺少有效结束标记"}
		}
		state.toolAttempts++
		if len(completion.ToolCalls) != 1 {
			if err := s.recordParallelToolRejection(ctx, question, state, round, completion.ToolCalls); err != nil {
				return qwen.Result{}, err
			}
			break
		}
		toolResult, forceFinalize, err := s.executeToolCall(ctx, question, state, round, completion.ToolCalls[0], progress)
		if err != nil {
			return qwen.Result{}, err
		}
		state.messages = append(state.messages, toolResult)
		if forceFinalize || state.toolAttempts >= maxToolAttempts {
			break
		}
	}
	if state.logicalCalls >= maxLogicalCalls {
		return qwen.Result{}, &RunError{Code: "MODEL_CALL_BUDGET_EXHAUSTED", Message: "问答模型调用预算已耗尽"}
	}
	if time.Until(question.DeadlineAt) <= finalizeReserve {
		return qwen.Result{}, &RunError{Code: "QA_TIMEOUT", Message: "问答超过总处理时限", Retryable: true}
	}
	if progress != nil {
		if err := progress("finalizing"); err != nil {
			return qwen.Result{}, err
		}
	}
	state.messages = append(state.messages, qwen.Message{Role: "user", Content: "请基于上方本次问题和证据，现在输出最终答案。只能使用已经提供的E编号作为引用；按answer_v1结构返回JSON，不调用工具。"})
	return s.callStructured(ctx, question, state, "final", 2048)
}

func (s *Service) runFixedRAG(ctx context.Context, question postgres.Question, state *agentState,
	progress ProgressFunc) (qwen.Result, error) {
	if s.generator == nil {
		return qwen.Result{}, &RunError{Code: "GENERATION_PROVIDER_NOT_CONFIGURED", Message: "问答生成服务未配置", Retryable: true}
	}
	if len(state.messages) != 2 {
		return qwen.Result{}, &RunError{Code: "QA_PROFILE_INVALID", Message: "固定RAG提示上下文无效"}
	}
	if err := ctx.Err(); err != nil {
		return qwen.Result{}, err
	}
	if time.Until(question.DeadlineAt) <= 5*time.Second {
		return qwen.Result{}, &RunError{Code: "QA_TIMEOUT", Message: "问答超过总处理时限", Retryable: true}
	}
	if progress != nil {
		if err := progress("generating"); err != nil {
			return qwen.Result{}, err
		}
	}
	seq := state.nextSeq
	state.nextSeq++
	inputHash, err := hashMessages(state.messages)
	if err != nil {
		return qwen.Result{}, err
	}
	if err := s.repository.BeginQuestionStep(ctx, question, seq, "GENERATION", inputHash); err != nil {
		return qwen.Result{}, err
	}
	beforeHTTP := state.httpAttempts
	completion, callErr := s.callModel(ctx, question, state, 5*time.Second, func(callCtx context.Context) (qwen.Result, error) {
		return s.generator.CompleteAnswer(callCtx, state.messages[0].Content, state.messages[1].Content)
	})
	state.logicalCalls++
	metadata := map[string]any{"phase": "fixed_rag_answer", "strategy": StrategyFixedRAG,
		"http_attempts": state.httpAttempts - beforeHTTP, "tool_calls": 0,
		"initial_evidence_count": len(state.evidence)}
	if callErr != nil {
		metadata["error_class"] = providerOutcome(callErr)
		encoded, _ := json.Marshal(metadata)
		_ = s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, providerRequestID(callErr),
			0, 0, 0, providerOutcome(callErr), encoded)
		return qwen.Result{}, providerRunError("FIXED_RAG_GENERATION_FAILED", "固定RAG生成调用失败", callErr)
	}
	encoded, _ := json.Marshal(metadata)
	if err := s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, completion.RequestID,
		completion.PromptTokens, completion.CompletionTokens, 0, completion.FinishReason, encoded); err != nil {
		return qwen.Result{}, err
	}
	return completion, nil
}

func (s *Service) callStructured(ctx context.Context, question postgres.Question, state *agentState, phase string, maxTokens int) (qwen.Result, error) {
	if state.logicalCalls >= maxLogicalCalls {
		return qwen.Result{}, &RunError{Code: "MODEL_CALL_BUDGET_EXHAUSTED", Message: "问答模型调用预算已耗尽"}
	}
	seq := state.nextSeq
	state.nextSeq++
	inputHash, err := hashMessages(state.messages)
	if err != nil {
		return qwen.Result{}, err
	}
	if err := s.repository.BeginQuestionStep(ctx, question, seq, "GENERATION", inputHash); err != nil {
		return qwen.Result{}, err
	}
	beforeHTTP := state.httpAttempts
	completion, callErr := s.callModel(ctx, question, state, 5*time.Second, func(callCtx context.Context) (qwen.Result, error) {
		return s.generator.CompleteStructured(callCtx, state.messages, maxTokens)
	})
	state.logicalCalls++
	metadata := map[string]any{"phase": phase, "logical_round": state.logicalCalls,
		"http_attempts": state.httpAttempts - beforeHTTP, "tools_disabled": true}
	encodedMetadata, _ := json.Marshal(metadata)
	if callErr != nil {
		_ = s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, providerRequestID(callErr),
			0, 0, 0, providerOutcome(callErr), encodedMetadata)
		return qwen.Result{}, providerRunError("GENERATION_REQUEST_FAILED", "问答生成请求失败", callErr)
	}
	if len(completion.ToolCalls) != 0 {
		_ = s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, completion.RequestID,
			completion.PromptTokens, completion.CompletionTokens, 0, "UNEXPECTED_TOOL_CALL", encodedMetadata)
		return qwen.Result{}, &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "最终回答阶段不允许工具调用"}
	}
	if err := s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash, completion.RequestID,
		completion.PromptTokens, completion.CompletionTokens, 0, completion.FinishReason, encodedMetadata); err != nil {
		return qwen.Result{}, err
	}
	return completion, nil
}

func (s *Service) callModel(ctx context.Context, question postgres.Question, state *agentState, reserve time.Duration,
	call func(context.Context) (qwen.Result, error)) (qwen.Result, error) {
	for attempt := 0; attempt < 2 && state.httpAttempts < maxModelHTTP; attempt++ {
		if err := ctx.Err(); err != nil {
			return qwen.Result{}, err
		}
		available := time.Until(question.DeadlineAt) - reserve
		if available <= 0 {
			return qwen.Result{}, context.DeadlineExceeded
		}
		callTimeout := min(40*time.Second, available)
		callCtx, cancel := context.WithTimeout(ctx, callTimeout)
		result, err := call(callCtx)
		cancel()
		state.httpAttempts++
		if err == nil {
			return result, nil
		}
		var apiError *qwen.APIError
		if !errors.As(err, &apiError) || !apiError.Retryable || attempt != 0 || state.retries >= 1 || state.httpAttempts >= maxModelHTTP {
			return qwen.Result{}, err
		}
		state.retries++
		select {
		case <-ctx.Done():
			return qwen.Result{}, ctx.Err()
		case <-time.After(200 * time.Millisecond):
		}
	}
	return qwen.Result{}, errors.New("model HTTP attempt budget exhausted")
}

func (s *Service) executeToolCall(ctx context.Context, question postgres.Question, state *agentState, round int,
	call qwen.ToolCall, progress ProgressFunc) (qwen.Message, bool, error) {
	if call.ID == "" || len(call.ID) > 128 || call.Type != "function" {
		return qwen.Message{}, false, &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "工具调用ID或类型无效"}
	}
	seq := state.nextSeq
	state.nextSeq++
	inputIdentity, _ := json.Marshal(struct {
		Round int           `json:"round"`
		Call  qwen.ToolCall `json:"call"`
	}{round, call})
	inputHash := sha256.Sum256(inputIdentity)
	if err := s.repository.BeginQuestionStep(ctx, question, seq, "TOOL", inputHash[:]); err != nil {
		return qwen.Message{}, false, err
	}
	metadata := map[string]any{"phase": "tool", "logical_round": round, "tool_name": call.Function.Name,
		"tool_call_id": call.ID, "tool_attempt": state.toolAttempts,
		"arguments": truncateAgentUTF8(call.Function.Arguments, 512)}
	observation := toolObservation{Status: "error", ResultID: *question.ResultID, Evidence: []toolEvidence{}}
	forceFinalize := false
	args, actionKey, argErr := parseToolArguments(call.Function.Name, call.Function.Arguments, state)
	if argErr != nil {
		observation.ErrorCode = "INVALID_TOOL_ARGUMENTS"
		metadata["error_code"] = observation.ErrorCode
	} else if _, repeated := state.seenActions[actionKey]; repeated {
		observation.ErrorCode = "REPEATED_ACTION"
		metadata["error_code"] = observation.ErrorCode
		forceFinalize = true
	} else {
		state.seenActions[actionKey] = struct{}{}
		toolReserve := 4 * time.Second
		if call.Function.Name == "search_video" {
			toolReserve = 40 * time.Second
		}
		if time.Until(question.DeadlineAt) <= finalizeReserve+toolReserve {
			observation.ErrorCode = "FINALIZE_BUDGET_RESERVED"
			metadata["error_code"] = observation.ErrorCode
			forceFinalize = true
		} else {
			if progress != nil {
				stage := "agent_searching"
				if call.Function.Name == "read_context" {
					stage = "agent_reading_context"
				}
				if err := progress(stage); err != nil {
					return qwen.Message{}, false, err
				}
			}
			added, truncated, requestID, usage, embeddingAttempts, execErr := s.runTool(ctx, question, state, call.Function.Name, args)
			state.queryEmbeddingAttempts += embeddingAttempts
			metadata["query_embedding_attempts"] = embeddingAttempts
			if requestID != "" {
				metadata["provider_request_id"] = requestID
			}
			if usage > 0 {
				metadata["usage_tokens"] = usage
			}
			if errors.Is(execErr, postgres.ErrQuestionExecutionLost) || errors.Is(execErr, context.Canceled) || errors.Is(execErr, context.DeadlineExceeded) && ctx.Err() != nil {
				return qwen.Message{}, false, execErr
			}
			if execErr != nil {
				observation.ErrorCode = "TOOL_EXECUTION_FAILED"
				metadata["error_code"] = observation.ErrorCode
			} else {
				observation.Status = "ok"
				observation.NewEvidenceCount = len(added)
				observation.Truncated = truncated || state.truncated
				for _, item := range added {
					observation.Evidence = append(observation.Evidence, toolEvidence{EvidenceID: item.ID, Text: item.Text,
						StartMS: item.StartMS, EndMS: item.EndMS})
				}
				metadata["new_evidence_count"] = len(added)
				metadata["new_evidence_sentence_seqs"] = questionEvidenceSeqs(added)
				metadata["truncated"] = observation.Truncated
				if len(added) == 0 || observation.Truncated || len(state.evidence) >= maxEvidenceCount || state.bytesUsed >= maxEvidenceBytes {
					forceFinalize = true
				}
			}
		}
	}
	encodedObservation, _ := json.Marshal(observation)
	encodedMetadata, _ := json.Marshal(metadata)
	if err := s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash[:], "", 0, 0, 0,
		observation.Status, encodedMetadata); err != nil {
		return qwen.Message{}, false, err
	}
	return qwen.Message{Role: "tool", ToolCallID: call.ID, Name: call.Function.Name,
		Content: string(encodedObservation)}, forceFinalize, nil
}

func (s *Service) recordParallelToolRejection(ctx context.Context, question postgres.Question, state *agentState,
	round int, calls []qwen.ToolCall) error {
	if len(calls) == 0 || len(calls) > 4 {
		return &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "模型返回了无法配对的工具调用数量"}
	}
	seq := state.nextSeq
	state.nextSeq++
	inputIdentity, _ := json.Marshal(struct {
		Round int             `json:"round"`
		Calls []qwen.ToolCall `json:"calls"`
	}{round, calls})
	inputHash := sha256.Sum256(inputIdentity)
	if err := s.repository.BeginQuestionStep(ctx, question, seq, "TOOL", inputHash[:]); err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(calls))
	for _, call := range calls {
		if call.ID == "" || len(call.ID) > 128 || call.Type != "function" {
			return &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "并行工具调用无法关联"}
		}
		if _, exists := seen[call.ID]; exists {
			return &RunError{Code: "MODEL_PROTOCOL_ERROR", Message: "并行工具调用ID重复"}
		}
		seen[call.ID] = struct{}{}
		observation, _ := json.Marshal(toolObservation{Status: "error", ResultID: *question.ResultID,
			Evidence: []toolEvidence{}, ErrorCode: "PARALLEL_TOOLS_NOT_ALLOWED"})
		state.messages = append(state.messages, qwen.Message{Role: "tool", ToolCallID: call.ID,
			Name: call.Function.Name, Content: string(observation)})
	}
	metadata, _ := json.Marshal(map[string]any{"phase": "tool", "logical_round": round,
		"outcome": "PARALLEL_TOOLS_NOT_ALLOWED", "returned_count": len(calls), "executed_count": 0})
	return s.repository.CompleteQuestionStepWithMetadata(ctx, question, seq, inputHash[:], "", 0, 0, 0,
		"PARALLEL_TOOLS_NOT_ALLOWED", metadata)
}

type toolObservation struct {
	Status           string         `json:"status"`
	ResultID         uuid.UUID      `json:"result_id"`
	NewEvidenceCount int            `json:"new_evidence_count"`
	Evidence         []toolEvidence `json:"evidence"`
	Truncated        bool           `json:"truncated"`
	ErrorCode        string         `json:"error_code,omitempty"`
}

type toolEvidence struct {
	EvidenceID string `json:"evidence_id"`
	Text       string `json:"text"`
	StartMS    int64  `json:"start_ms"`
	EndMS      int64  `json:"end_ms"`
}

func parseToolArguments(name, raw string, state *agentState) (any, string, error) {
	if len(raw) == 0 || len(raw) > 2048 || !utf8.ValidString(raw) || rejectDuplicateJSONKeys([]byte(raw)) != nil {
		return nil, "", errors.New("tool arguments are not valid JSON")
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(raw)))
	decoder.DisallowUnknownFields()
	var args any
	switch name {
	case "search_video":
		var input struct {
			Query string `json:"query"`
		}
		if err := decoder.Decode(&input); err != nil {
			return nil, "", err
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, "", errors.New("trailing tool argument data")
		}
		input.Query = strings.TrimSpace(input.Query)
		if input.Query == "" || len(input.Query) > 512 || strings.ContainsRune(input.Query, '\x00') || !utf8.ValidString(input.Query) {
			return nil, "", errors.New("search query is invalid")
		}
		args = input
	case "read_context":
		var input struct {
			EvidenceID string `json:"evidence_id"`
			Before     int    `json:"before"`
			After      int    `json:"after"`
		}
		if err := decoder.Decode(&input); err != nil {
			return nil, "", err
		}
		if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
			return nil, "", errors.New("trailing tool argument data")
		}
		if !evidenceIDValid(input.EvidenceID) || input.Before < 0 || input.Before > 3 || input.After < 0 || input.After > 3 ||
			input.Before+input.After == 0 {
			return nil, "", errors.New("context range is invalid")
		}
		if _, ok := state.byEvidenceID[input.EvidenceID]; !ok {
			return nil, "", errors.New("evidence ID is not registered for this question")
		}
		args = input
	default:
		return nil, "", errors.New("tool is not permitted")
	}
	canonical, err := json.Marshal(struct {
		Name string `json:"name"`
		Args any    `json:"args"`
	}{name, args})
	if err != nil {
		return nil, "", err
	}
	sum := sha256.Sum256(canonical)
	return args, fmt.Sprintf("%x", sum[:]), nil
}

func (s *Service) runTool(ctx context.Context, question postgres.Question, state *agentState, name string, args any) ([]postgres.QuestionEvidence, bool, string, int64, int, error) {
	var candidates []postgres.RetrievedSentence
	var requestID string
	var usage int64
	embedAttempts := 0
	switch name {
	case "search_video":
		input := args.(struct {
			Query string `json:"query"`
		})
		if s.embedder == nil {
			return nil, false, "", 0, 0, errors.New("Embedding provider is not configured")
		}
		remainingAttempts := 6 - state.queryEmbeddingAttempts
		if remainingAttempts < 1 {
			return nil, false, "", 0, 0, errors.New("query embedding attempt budget exhausted")
		}
		result, embedAttempts, err := s.embedQueryWithRetry(ctx, input.Query, min(2, remainingAttempts))
		if err != nil {
			return nil, false, providerRequestID(err), 0, embedAttempts, err
		}
		vectors, err := embedding.ValidateVectors(result, 1)
		if err != nil {
			return nil, false, result.RequestID, result.UsageTokens, embedAttempts, err
		}
		requestID, usage = result.RequestID, result.UsageTokens
		dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		primary, err := s.repository.SearchEvidence(dbCtx, question.VideoID, *question.TranscriptID,
			*question.EmbeddingSetID, vectors[0], 8)
		cancel()
		if err != nil {
			return nil, false, requestID, usage, embedAttempts, err
		}
		candidates = primary
	case "read_context":
		input := args.(struct {
			EvidenceID string `json:"evidence_id"`
			Before     int    `json:"before"`
			After      int    `json:"after"`
		})
		anchor := state.byEvidenceID[input.EvidenceID]
		dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		rows, err := s.repository.ReadEvidenceContext(dbCtx, *question.TranscriptID, anchor.SentenceSeq, input.Before, input.After)
		cancel()
		if err != nil {
			return nil, false, "", 0, 0, err
		}
		candidates = rows
	default:
		return nil, false, "", 0, 0, errors.New("tool is not permitted")
	}
	added, truncated, err := s.prepareEvidence(ctx, *question.TranscriptID, candidates, state.evidence, name != "read_context")
	if err != nil {
		return nil, false, requestID, usage, embedAttempts, err
	}
	if len(added) > 0 {
		if err := s.repository.RegisterEvidence(ctx, question, added); err != nil {
			return nil, false, requestID, usage, embedAttempts, &RunError{Code: "EVIDENCE_SAVE_FAILED", Message: "补充证据保存失败", Retryable: true, Cause: err}
		}
		for _, item := range added {
			state.evidence = append(state.evidence, item)
			state.byEvidenceID[item.ID] = item
			state.bySentence[item.SentenceSeq] = item.ID
			state.bytesUsed += len(item.Text)
		}
	}
	state.truncated = state.truncated || truncated
	return added, truncated, requestID, usage, embedAttempts, nil
}

func (s *Service) embedQueryWithRetry(ctx context.Context, query string, maxAttempts int) (embedding.Result, int, error) {
	var lastErr error
	if maxAttempts < 1 || maxAttempts > 2 {
		return embedding.Result{}, 0, errors.New("query embedding attempts exceed the configured bound")
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		callCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
		result, err := s.embedder.EmbedQuery(callCtx, query)
		cancel()
		if err == nil {
			return result, attempt + 1, nil
		}
		lastErr = err
		var apiError *embedding.APIError
		if !errors.As(err, &apiError) || !apiError.Retryable || attempt+1 >= maxAttempts {
			return embedding.Result{}, attempt + 1, err
		}
		delay := apiError.RetryAfter
		if delay <= 0 {
			delay = 200 * time.Millisecond
		}
		delay = min(delay, 2*time.Second)
		select {
		case <-ctx.Done():
			return embedding.Result{}, attempt + 1, ctx.Err()
		case <-time.After(delay):
		}
	}
	return embedding.Result{}, maxAttempts, lastErr
}

func (s *Service) prepareEvidence(ctx context.Context, transcriptID uuid.UUID, primary []postgres.RetrievedSentence,
	existing []postgres.QuestionEvidence, expand bool) ([]postgres.QuestionEvidence, bool, error) {
	seqs := make([]int, 0, len(primary))
	seen := make(map[int]struct{}, len(primary))
	for _, sentence := range primary {
		if sentence.TranscriptID != transcriptID || sentence.SentenceSeq <= 0 || sentence.Text == "" || !utf8.ValidString(sentence.Text) {
			return nil, false, &RunError{Code: "RETRIEVAL_SOURCE_INVALID", Message: "检索结果来源与固定字幕不一致"}
		}
		if _, ok := seen[sentence.SentenceSeq]; !ok {
			seen[sentence.SentenceSeq] = struct{}{}
			seqs = append(seqs, sentence.SentenceSeq)
			if len(seqs) == 8 {
				break
			}
		}
	}
	if len(seqs) == 0 {
		return []postgres.QuestionEvidence{}, false, nil
	}
	expanded := primary
	if expand {
		dbCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
		var err error
		expanded, err = s.repository.ExpandEvidence(dbCtx, transcriptID, seqs)
		cancel()
		if err != nil {
			return nil, false, &RunError{Code: "EVIDENCE_CONTEXT_FAILED", Message: "读取检索片段上下文失败", Retryable: true, Cause: err}
		}
	}
	bySeq := make(map[int]postgres.RetrievedSentence, len(expanded))
	for _, sentence := range expanded {
		bySeq[sentence.SentenceSeq] = sentence
	}
	existingSeqs := make(map[int]struct{}, len(existing))
	for _, item := range existing {
		existingSeqs[item.SentenceSeq] = struct{}{}
	}
	ordered := make([]postgres.RetrievedSentence, 0, len(seqs)*3)
	seen = make(map[int]struct{}, len(seqs)*3)
	for _, seq := range seqs {
		candidateSeqs := []int{seq}
		if expand {
			candidateSeqs = []int{seq - 1, seq, seq + 1}
		}
		for _, candidateSeq := range candidateSeqs {
			if candidateSeq <= 0 {
				continue
			}
			if _, ok := seen[candidateSeq]; ok {
				continue
			}
			seen[candidateSeq] = struct{}{}
			if _, old := existingSeqs[candidateSeq]; old {
				continue
			}
			if candidate, ok := bySeq[candidateSeq]; ok && candidate.TranscriptID == transcriptID && utf8.ValidString(candidate.Text) {
				ordered = append(ordered, candidate)
			}
		}
	}
	newEvidence := make([]postgres.QuestionEvidence, 0, min(len(ordered), maxEvidenceCount-len(existing)))
	bytesUsed := 0
	for _, item := range existing {
		bytesUsed += len(item.Text)
	}
	truncated := false
	for _, sentence := range ordered {
		if len(existing)+len(newEvidence) >= maxEvidenceCount || bytesUsed+len(sentence.Text) > maxEvidenceBytes {
			truncated = true
			continue
		}
		hash := sha256.Sum256([]byte(sentence.Text))
		newEvidence = append(newEvidence, postgres.QuestionEvidence{ID: fmt.Sprintf("E%d", len(existing)+len(newEvidence)+1),
			TranscriptID: sentence.TranscriptID, SentenceSeq: sentence.SentenceSeq, Text: sentence.Text,
			StartMS: sentence.StartMS, EndMS: sentence.EndMS, ContentHash: hash[:]})
		bytesUsed += len(sentence.Text)
	}
	return newEvidence, truncated, nil
}

func hashMessages(messages []qwen.Message) ([]byte, error) {
	encoded, err := json.Marshal(messages)
	if err != nil {
		return nil, fmt.Errorf("encode question model messages: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return sum[:], nil
}

func providerOutcome(err error) string {
	var apiError *qwen.APIError
	if errors.As(err, &apiError) {
		if apiError.StatusCode == 429 {
			return "RATE_LIMITED"
		}
		return "PROVIDER_FAILED"
	}
	var embeddingError *embedding.APIError
	if errors.As(err, &embeddingError) {
		if embeddingError.StatusCode == 429 {
			return "RATE_LIMITED"
		}
		return "PROVIDER_FAILED"
	}
	return "TRANSPORT_UNKNOWN"
}

func providerRequestID(err error) string {
	var apiError *qwen.APIError
	if errors.As(err, &apiError) {
		return apiError.RequestID
	}
	var embeddingError *embedding.APIError
	if errors.As(err, &embeddingError) {
		return embeddingError.RequestID
	}
	return ""
}

func truncateAgentUTF8(value string, maxBytes int) string {
	if len(value) <= maxBytes {
		return value
	}
	value = value[:maxBytes]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}
