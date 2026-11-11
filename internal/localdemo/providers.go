// Package localdemo contains deterministic offline fixtures for the explicitly
// enabled local interview demo. It does not emulate semantic model behavior.
package localdemo

import (
	"context"
	"strings"

	"mirrorcore/internal/providers/embedding"
	"mirrorcore/internal/providers/qwen"
)

type Embedder struct{}

func (Embedder) EmbedQuery(ctx context.Context, query string) (embedding.Result, error) {
	vector := make([]float64, embedding.Dimension)
	index := 0
	if query == "执行 epoch" {
		index = 1
	}
	vector[index] = 1
	return embedding.Result{RequestID: "local-demo-unit-vector", Items: []embedding.Item{{Index: 0, Embedding: vector}}}, nil
}

func (Embedder) EmbedDocuments(_ context.Context, texts []string) (embedding.Result, error) {
	result := embedding.Result{RequestID: "local-demo-document-embedding-v1"}
	for index, text := range texts {
		vector := make([]float64, embedding.Dimension)
		if strings.Contains(strings.ToLower(text), "epoch") {
			vector[1] = 1
		} else {
			vector[0] = 1
		}
		result.Items = append(result.Items, embedding.Item{Index: index, Embedding: vector})
	}
	return result, nil
}

type Generator struct{}

func (Generator) CompleteDecision(_ context.Context, messages []qwen.Message) (qwen.Result, error) {
	for _, message := range messages {
		if message.Role == "tool" {
			return qwen.Result{RequestID: "local-demo-decision-2", FinishReason: "stop"}, nil
		}
	}
	return qwen.Result{RequestID: "local-demo-decision-1", FinishReason: "tool_calls", ToolCalls: []qwen.ToolCall{{
		ID: "local-demo-search-1", Type: "function",
		Function: qwen.ToolFunction{Name: "search_video", Arguments: `{"query":"执行 epoch"}`},
	}}}, nil
}

func (Generator) CompleteAnswer(context.Context, string, string) (qwen.Result, error) {
	return answerResult(), nil
}

func (Generator) CompleteStructured(context.Context, []qwen.Message, int) (qwen.Result, error) {
	return answerResult(), nil
}

func answerResult() qwen.Result {
	return qwen.Result{
		RequestID:    "local-demo-answer",
		FinishReason: "stop",
		Content:      `{"status":"answered","answer_blocks":[{"text":"【本地离线演示固定答案】Kafka通过消息队列解耦API与后台Worker；执行epoch在任务被接管时递增，用于拒绝旧执行者提交结果。","evidence_refs":["E1","E2"]}],"unanswered_points":[]}`,
	}
}
