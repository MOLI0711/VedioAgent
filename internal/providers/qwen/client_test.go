package qwen

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestCompleteDecisionSendsRestrictedToolsAndPreservesCallIDs(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/compatible-mode/v1/chat/completions" {
			t.Errorf("request = %s %s, want POST chat completions", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", got)
		}
		var request chatRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Model != Model || request.EnableThinking || request.Temperature != 0 || request.Stream ||
			request.MaxTokens != 768 || request.ToolChoice != "auto" || request.ParallelTools == nil || *request.ParallelTools {
			t.Errorf("unexpected decision request settings: %+v", request)
		}
		if len(request.Tools) != 2 || request.Tools[0].Function.Name != "search_video" ||
			request.Tools[1].Function.Name != "read_context" {
			t.Errorf("unexpected tool allowlist: %+v", request.Tools)
		}
		if len(request.Messages) != 4 || request.Messages[2].ToolCalls[0].ID != "call-previous" ||
			request.Messages[3].ToolCallID != "call-previous" {
			t.Errorf("assistant/tool call ID pairing was not preserved: %+v", request.Messages)
		}
		_, _ = io.WriteString(w, `{"id":"request-1","choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":"","tool_calls":[{"id":"call-next","type":"function","function":{"name":"search_video","arguments":"{\"query\":\"missing detail\"}"}}]}}],"usage":{"prompt_tokens":10,"completion_tokens":4}}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL+"/compatible-mode/v1/chat/completions", server.Client())
	messages := []Message{
		{Role: "system", Content: "system prompt"},
		{Role: "user", Content: "question"},
		{Role: "assistant", ToolCalls: []ToolCall{{ID: "call-previous", Type: "function",
			Function: ToolFunction{Name: "search_video", Arguments: `{"query":"earlier"}`}}}},
		{Role: "tool", ToolCallID: "call-previous", Name: "search_video", Content: `{"evidence_ids":[]}`},
	}

	result, err := client.CompleteDecision(context.Background(), messages)
	if err != nil {
		t.Fatalf("CompleteDecision() error = %v", err)
	}
	if result.RequestID != "request-1" || result.FinishReason != "tool_calls" ||
		result.PromptTokens != 10 || result.CompletionTokens != 4 || len(result.ToolCalls) != 1 {
		t.Fatalf("CompleteDecision() result = %+v", result)
	}
	if result.ToolCalls[0].ID != "call-next" || result.ToolCalls[0].Function.Name != "search_video" ||
		result.ToolCalls[0].Function.Arguments != `{"query":"missing detail"}` {
		t.Fatalf("tool call = %+v", result.ToolCalls[0])
	}
}

func TestCompleteAnswerUsesStrictJSONSchemaAndThinkingDisabled(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request chatRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Model != Model || request.EnableThinking || request.Temperature != 0 || request.Stream ||
			request.MaxTokens != 2048 || request.ResponseFormat == nil ||
			request.ResponseFormat.Type != "json_schema" || !request.ResponseFormat.JSONSchema.Strict || len(request.Tools) != 0 {
			t.Errorf("unexpected structured request: %+v", request)
		}
		_, _ = io.WriteString(w, `{"id":"request-2","choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"{\"status\":\"insufficient\",\"answer_blocks\":[],\"unanswered_points\":[\"x\"]}"}}],"usage":{"prompt_tokens":20,"completion_tokens":8}}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, server.Client())

	result, err := client.CompleteAnswer(context.Background(), "system", "user")
	if err != nil {
		t.Fatalf("CompleteAnswer() error = %v", err)
	}
	if result.Content != `{"status":"insufficient","answer_blocks":[],"unanswered_points":["x"]}` ||
		result.RequestID != "request-2" || result.PromptTokens != 20 || result.CompletionTokens != 8 {
		t.Fatalf("CompleteAnswer() result = %+v", result)
	}
}

func TestCompleteClassifiesRateLimitAndRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "5")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"id":"request-429","code":"Throttling"}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, server.Client())

	_, err := client.CompleteAnswer(context.Background(), "system", "user")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("CompleteAnswer() error = %v, want *APIError", err)
	}
	if apiErr.StatusCode != http.StatusTooManyRequests || apiErr.Code != "Throttling" ||
		apiErr.RequestID != "request-429" || !apiErr.Retryable || apiErr.RetryAfter != 5*time.Second {
		t.Fatalf("APIError = %+v", apiErr)
	}
}

func newTestClient(t *testing.T, endpoint string, httpClient *http.Client) *Client {
	t.Helper()
	client, err := New("test-workspace", "test-api-key")
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	client.endpoint = endpoint
	client.http = httpClient
	return client
}
