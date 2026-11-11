package qwen

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	Model          = "qwen3.7-plus-2026-05-26"
	MaxResponse    = 1 << 20
	requestTimeout = 60 * time.Second
)

type APIError struct {
	StatusCode int
	Code       string
	RequestID  string
	Retryable  bool
	RetryAfter time.Duration
}

func (e *APIError) Error() string {
	return fmt.Sprintf("Bailian chat request failed with HTTP %d (%s)", e.StatusCode, e.Code)
}

type Result struct {
	RequestID        string
	Content          string
	FinishReason     string
	PromptTokens     int64
	CompletionTokens int64
	ToolCalls        []ToolCall
}

// Message is a Chat Completions message. Tool call messages are passed back to
// the provider unchanged so every tool result stays paired with its call ID.
type Message struct {
	Role       string     `json:"role"`
	Content    string     `json:"content"`
	ToolCalls  []ToolCall `json:"tool_calls,omitempty"`
	ToolCallID string     `json:"tool_call_id,omitempty"`
	Name       string     `json:"name,omitempty"`
}

type ToolCall struct {
	ID       string       `json:"id"`
	Type     string       `json:"type"`
	Function ToolFunction `json:"function"`
}

type ToolFunction struct {
	Name      string `json:"name"`
	Arguments string `json:"arguments"`
}

type Tool struct {
	Type     string   `json:"type"`
	Function ToolSpec `json:"function"`
}

type ToolSpec struct {
	Name        string         `json:"name"`
	Description string         `json:"description"`
	Parameters  map[string]any `json:"parameters"`
}

type Client struct {
	endpoint string
	apiKey   string
	http     *http.Client
}

func New(workspaceID, apiKey string) (*Client, error) {
	if workspaceID == "" || apiKey == "" || len(workspaceID) > 128 || len(apiKey) > 512 {
		return nil, errors.New("Qwen provider requires a valid workspace ID and API key")
	}
	for _, r := range workspaceID {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-') {
			return nil, errors.New("Qwen workspace ID contains invalid characters")
		}
	}
	endpoint := "https://" + workspaceID + ".cn-beijing.maas.aliyuncs.com/compatible-mode/v1/chat/completions"
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Scheme != "https" || parsed.Host == "" {
		return nil, errors.New("Qwen endpoint is invalid")
	}
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		MaxIdleConns:          16,
		MaxIdleConnsPerHost:   4,
		MaxConnsPerHost:       4,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 55 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: requestTimeout,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return &Client{endpoint: endpoint, apiKey: apiKey, http: client}, nil
}

type chatRequest struct {
	Model          string          `json:"model"`
	Messages       []Message       `json:"messages"`
	EnableThinking bool            `json:"enable_thinking"`
	Temperature    float64         `json:"temperature"`
	MaxTokens      int             `json:"max_tokens"`
	Stream         bool            `json:"stream"`
	ResponseFormat *responseFormat `json:"response_format,omitempty"`
	Tools          []Tool          `json:"tools,omitempty"`
	ToolChoice     string          `json:"tool_choice,omitempty"`
	ParallelTools  *bool           `json:"parallel_tool_calls,omitempty"`
}

type responseFormat struct {
	Type       string    `json:"type"`
	JSONSchema schemaBox `json:"json_schema"`
}

type schemaBox struct {
	Name   string         `json:"name"`
	Strict bool           `json:"strict"`
	Schema map[string]any `json:"schema"`
}

type chatResponse struct {
	ID      string `json:"id"`
	Code    string `json:"code"`
	Message string `json:"message"`
	Choices []struct {
		FinishReason string `json:"finish_reason"`
		Message      struct {
			Role      string     `json:"role"`
			Content   string     `json:"content"`
			ToolCalls []ToolCall `json:"tool_calls"`
		} `json:"message"`
	} `json:"choices"`
	Usage struct {
		PromptTokens     int64 `json:"prompt_tokens"`
		CompletionTokens int64 `json:"completion_tokens"`
	} `json:"usage"`
}

func (c *Client) CompleteAnswer(ctx context.Context, systemPrompt, userPrompt string) (Result, error) {
	if strings.TrimSpace(systemPrompt) == "" || strings.TrimSpace(userPrompt) == "" {
		return Result{}, errors.New("Qwen prompts must not be empty")
	}
	return c.CompleteStructured(ctx, []Message{{Role: "system", Content: systemPrompt}, {Role: "user", Content: userPrompt}}, 2048)
}

func (c *Client) CompleteDecision(ctx context.Context, messages []Message) (Result, error) {
	if len(messages) < 2 {
		return Result{}, errors.New("Qwen decision requires system and user messages")
	}
	parallel := false
	return c.complete(ctx, chatRequest{
		Messages: messages, MaxTokens: 768, Tools: agentTools(), ToolChoice: "auto", ParallelTools: &parallel,
	})
}

func (c *Client) CompleteStructured(ctx context.Context, messages []Message, maxTokens int) (Result, error) {
	return c.CompleteStructuredSchema(ctx, messages, "mirrorcore_answer", answerSchema(), maxTokens)
}

func (c *Client) CompleteStructuredSchema(ctx context.Context, messages []Message, schemaName string,
	schema map[string]any, maxTokens int) (Result, error) {
	if len(messages) < 2 || maxTokens < 1 || maxTokens > 8192 || schemaName == "" || len(schemaName) > 64 || schema == nil {
		return Result{}, errors.New("Qwen structured request is invalid")
	}
	return c.complete(ctx, chatRequest{
		Messages: messages, MaxTokens: maxTokens,
		ResponseFormat: &responseFormat{Type: "json_schema", JSONSchema: schemaBox{
			Name: schemaName, Strict: true, Schema: schema,
		}},
	})
}

func (c *Client) complete(ctx context.Context, body chatRequest) (Result, error) {
	if c == nil || c.http == nil || c.endpoint == "" || c.apiKey == "" {
		return Result{}, errors.New("Qwen client is not initialized")
	}
	body.Model = Model
	body.EnableThinking = false
	body.Temperature = 0
	body.Stream = false
	if body.MaxTokens == 0 {
		body.MaxTokens = 2048
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return Result{}, fmt.Errorf("encode Qwen request: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(encoded))
	if err != nil {
		return Result{}, fmt.Errorf("create Qwen request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+c.apiKey)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	response, err := c.http.Do(request)
	if err != nil {
		return Result{}, &APIError{Code: "TRANSPORT_ERROR", Retryable: true}
	}
	defer response.Body.Close()
	responseBytes, err := readBounded(response.Body, MaxResponse)
	if err != nil {
		return Result{}, err
	}
	var decoded chatResponse
	if json.Unmarshal(responseBytes, &decoded) != nil {
		return Result{}, errors.New("Qwen response is not valid JSON")
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		code := decoded.Code
		if code == "" {
			code = http.StatusText(response.StatusCode)
		}
		return Result{}, &APIError{StatusCode: response.StatusCode, Code: safeCode(code), RequestID: decoded.ID,
			Retryable:  response.StatusCode == http.StatusTooManyRequests || response.StatusCode == http.StatusRequestTimeout || response.StatusCode >= 500,
			RetryAfter: parseRetryAfter(response.Header.Get("Retry-After"))}
	}
	if decoded.Code != "" || len(decoded.Choices) != 1 ||
		decoded.Usage.PromptTokens < 0 || decoded.Usage.CompletionTokens < 0 {
		return Result{}, errors.New("Qwen response is incomplete or contains an error")
	}
	choice := decoded.Choices[0]
	if choice.Message.Content == "" && len(choice.Message.ToolCalls) == 0 {
		return Result{}, errors.New("Qwen response contains neither content nor tool calls")
	}
	return Result{RequestID: decoded.ID, Content: choice.Message.Content, ToolCalls: choice.Message.ToolCalls,
		FinishReason: choice.FinishReason, PromptTokens: decoded.Usage.PromptTokens,
		CompletionTokens: decoded.Usage.CompletionTokens}, nil
}

func parseRetryAfter(value string) time.Duration {
	if seconds, err := strconv.Atoi(strings.TrimSpace(value)); err == nil && seconds > 0 {
		return min(time.Duration(seconds)*time.Second, 15*time.Minute)
	}
	if at, err := http.ParseTime(value); err == nil {
		if delay := time.Until(at); delay > 0 {
			return min(delay, 15*time.Minute)
		}
	}
	return 0
}

func agentTools() []Tool {
	return []Tool{
		{Type: "function", Function: ToolSpec{Name: "search_video",
			Description: "围绕当前问题缺失的信息，在固定视频结果中重新检索语音证据。",
			Parameters: map[string]any{"type": "object", "properties": map[string]any{
				"query": map[string]any{"type": "string"},
			}, "required": []string{"query"}, "additionalProperties": false},
		}},
		{Type: "function", Function: ToolSpec{Name: "read_context",
			Description: "读取本次已注册证据前后的同版本字幕。",
			Parameters: map[string]any{"type": "object", "properties": map[string]any{
				"evidence_id": map[string]any{"type": "string"},
				"before":      map[string]any{"type": "integer", "minimum": 0, "maximum": 3},
				"after":       map[string]any{"type": "integer", "minimum": 0, "maximum": 3},
			}, "required": []string{"evidence_id", "before", "after"}, "additionalProperties": false},
		}},
	}
}

func answerSchema() map[string]any {
	stringArray := map[string]any{"type": "array", "items": map[string]any{"type": "string"}}
	block := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"text":          map[string]any{"type": "string"},
			"evidence_refs": stringArray,
		},
		"required":             []string{"text", "evidence_refs"},
		"additionalProperties": false,
	}
	return map[string]any{
		"type": "object",
		"properties": map[string]any{
			"status":            map[string]any{"type": "string", "enum": []string{"answered", "partial", "insufficient"}},
			"answer_blocks":     map[string]any{"type": "array", "items": block},
			"unanswered_points": stringArray,
		},
		"required":             []string{"status", "answer_blocks", "unanswered_points"},
		"additionalProperties": false,
	}
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, errors.New("read Qwen response failed")
	}
	if int64(len(body)) > limit {
		return nil, errors.New("Qwen response exceeds the size limit")
	}
	return body, nil
}

func safeCode(code string) string {
	code = strings.TrimSpace(code)
	if len(code) > 96 {
		code = code[:96]
	}
	return code
}
