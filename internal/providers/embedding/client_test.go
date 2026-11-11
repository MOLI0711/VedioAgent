package embedding

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

func TestEmbedDocumentsSendsModelAndMapsIndexedVectors(t *testing.T) {
	vector := make([]float64, Dimension)
	for i := range vector {
		vector[i] = float64(i+1) / Dimension
	}
	response, err := json.Marshal(map[string]any{
		"request_id": "embedding-request-1",
		"output": map[string]any{"embeddings": []any{
			map[string]any{"index": 1, "embedding": vector},
			map[string]any{"index": 0, "embedding": vector},
		}},
		"usage": map[string]any{"total_tokens": 17},
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/services/embeddings/text-embedding/text-embedding" {
			t.Errorf("request = %s %s, want POST Embedding endpoint", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		var request requestBody
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Model != Model || request.Parameters.Dimension != Dimension ||
			request.Parameters.TextType != "document" || request.Parameters.OutputType != "dense" ||
			len(request.Input.Texts) != 2 || request.Input.Texts[0] != "first" || request.Input.Texts[1] != "second" {
			t.Errorf("unexpected request: %+v", request)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(response)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/api/v1/services/embeddings/text-embedding/text-embedding", server.Client())
	result, err := client.EmbedDocuments(context.Background(), []string{"first", "second"})
	if err != nil {
		t.Fatalf("EmbedDocuments() error = %v", err)
	}
	if result.RequestID != "embedding-request-1" || result.UsageTokens != 17 || len(result.Items) != 2 {
		t.Fatalf("EmbedDocuments() result = %+v", result)
	}
	vectors, err := ValidateVectors(result, 2)
	if err != nil {
		t.Fatalf("ValidateVectors() error = %v", err)
	}
	if len(vectors) != 2 || vectors[0][0] != float32(vector[0]) || vectors[1][Dimension-1] != float32(vector[Dimension-1]) {
		t.Fatal("ValidateVectors() did not restore request order by response index")
	}
}

func TestEmbedQueryUsesQueryTextTypeAndParsesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var request requestBody
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if request.Parameters.TextType != "query" || len(request.Input.Texts) != 1 || request.Input.Texts[0] != "question" {
			t.Errorf("unexpected query request: %+v", request)
		}
		w.Header().Set("Retry-After", "4")
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = io.WriteString(w, `{"request_id":"request-429","code":"Throttling","message":"rate limited"}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, server.Client())
	_, err := client.EmbedQuery(context.Background(), "question")
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("EmbedQuery() error = %v, want *APIError", err)
	}
	if apiErr.StatusCode != http.StatusTooManyRequests || !apiErr.Retryable ||
		apiErr.RequestID != "request-429" || apiErr.RetryAfter != 4*time.Second {
		t.Fatalf("APIError = %+v", apiErr)
	}
}

func TestEmbedRejectsInvalidBatchBeforeSending(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		t.Error("provider request sent for invalid input")
	}))
	defer server.Close()
	client := newTestClient(t, server.URL, server.Client())
	if _, err := client.EmbedDocuments(context.Background(), nil); err == nil {
		t.Fatal("EmbedDocuments() accepted an empty batch")
	}
	if _, err := client.EmbedQuery(context.Background(), ""); err == nil {
		t.Fatal("EmbedQuery() accepted an empty query")
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
