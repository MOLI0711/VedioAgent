package funasr

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSubmitUsesAsyncTranscriptionContract(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/services/audio/asr/transcription" {
			t.Errorf("request = %s %s, want POST transcription endpoint", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", got)
		}
		if got := r.Header.Get("X-DashScope-Async"); got != "enable" {
			t.Errorf("X-DashScope-Async = %q, want enable", got)
		}
		if got := r.Header.Get("Content-Type"); got != "application/json" {
			t.Errorf("Content-Type = %q", got)
		}
		var body struct {
			Model string `json:"model"`
			Input struct {
				FileURLs []string `json:"file_urls"`
			} `json:"input"`
			Parameters struct {
				ChannelID []int `json:"channel_id"`
			} `json:"parameters"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode request body: %v", err)
		}
		if body.Model != "fun-asr" || len(body.Input.FileURLs) != 1 ||
			body.Input.FileURLs[0] != "https://objects.example.test/audio.wav" ||
			len(body.Parameters.ChannelID) != 1 || body.Parameters.ChannelID[0] != 0 {
			t.Errorf("unexpected request body: %+v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"request_id":"request-1","output":{"task_id":"task-1","task_status":"PENDING"}}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL+"/api/v1", server.Client())
	task, err := client.Submit(context.Background(), "https://objects.example.test/audio.wav")
	if err != nil {
		t.Fatalf("Submit() error = %v", err)
	}
	if task.ID != "task-1" || task.RequestID != "request-1" || task.Status != "PENDING" {
		t.Fatalf("Submit() task = %+v", task)
	}
}

func TestSubmitClassifiesUnknownAcceptanceAndRetryableErrors(t *testing.T) {
	tests := []struct {
		name       string
		status     int
		header     string
		body       string
		ambiguous  bool
		retryAfter time.Duration
	}{
		{name: "success without task ID", status: http.StatusOK, body: `{"output":{}}`, ambiguous: true},
		{name: "rate limited", status: http.StatusTooManyRequests, header: "3", body: `{"code":"Throttling","message":"slow down"}`, retryAfter: 3 * time.Second},
		{name: "server error is ambiguous", status: http.StatusBadGateway, body: `{"code":"BadGateway"}`, ambiguous: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if test.header != "" {
					w.Header().Set("Retry-After", test.header)
				}
				w.WriteHeader(test.status)
				_, _ = io.WriteString(w, test.body)
			}))
			defer server.Close()
			client := newTestClient(t, server.URL+"/api/v1", server.Client())
			_, err := client.Submit(context.Background(), "https://objects.example.test/audio.wav")
			var apiErr *APIError
			if !errors.As(err, &apiErr) {
				t.Fatalf("Submit() error = %v, want *APIError", err)
			}
			if apiErr.Ambiguous != test.ambiguous || apiErr.RetryAfter != test.retryAfter {
				t.Fatalf("APIError = %+v, want ambiguous=%t retryAfter=%s", apiErr, test.ambiguous, test.retryAfter)
			}
		})
	}
}

func TestGetTaskMapsTaskAndSubtaskFields(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/tasks/task-1" {
			t.Errorf("request = %s %s, want GET task endpoint", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer test-api-key" {
			t.Errorf("Authorization = %q", got)
		}
		_, _ = io.WriteString(w, `{"request_id":"request-2","output":{"task_id":"task-1","task_status":"SUCCEEDED","results":[{"subtask_status":"SUCCEEDED","transcription_url":"https://result.aliyuncs.com/transcript.json","code":"","message":""}]}}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL+"/api/v1", server.Client())

	task, err := client.GetTask(context.Background(), "task-1")
	if err != nil {
		t.Fatalf("GetTask() error = %v", err)
	}
	if task.ID != "task-1" || task.RequestID != "request-2" || task.Status != "SUCCEEDED" ||
		task.SubtaskStatus != "SUCCEEDED" || task.ResultURL != "https://result.aliyuncs.com/transcript.json" {
		t.Fatalf("GetTask() task = %+v", task)
	}
}

func TestGetTaskRejectsMismatchedTaskIDAndUnsafePath(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, `{"output":{"task_id":"another-task","task_status":"SUCCEEDED"}}`)
	}))
	defer server.Close()
	client := newTestClient(t, server.URL+"/api/v1", server.Client())

	if _, err := client.GetTask(context.Background(), "task-1"); err == nil || !strings.Contains(err.Error(), "did not confirm") {
		t.Fatalf("GetTask() mismatch error = %v", err)
	}
	if _, err := client.GetTask(context.Background(), "../other"); err == nil {
		t.Fatal("GetTask() accepted task ID containing a path separator")
	}
}

func TestDownloadResultValidatesHostAndTranscript(t *testing.T) {
	client, err := New("test-workspace", "test-api-key")
	if err != nil {
		t.Fatal(err)
	}
	resultBody := `{"transcripts":[{"sentences":[{"begin_time":12,"end_time":34,"text":"hello"}]}]}`
	client.http = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host != "result.aliyuncs.com" || r.URL.Scheme != "https" {
			t.Fatalf("download request URL = %s, want allowed HTTPS Aliyun host", r.URL)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     make(http.Header),
			Body:       io.NopCloser(strings.NewReader(resultBody)),
			Request:    r,
		}, nil
	})}

	body, err := client.DownloadResult(context.Background(), "https://result.aliyuncs.com/transcript.json")
	if err != nil {
		t.Fatalf("DownloadResult() error = %v", err)
	}
	if err := ValidateResult(body); err != nil {
		t.Fatalf("ValidateResult() error = %v", err)
	}
	for _, unsafeURL := range []string{
		"http://result.aliyuncs.com/transcript.json",
		"https://result.aliyuncs.com.evil.test/transcript.json",
		"https://user:pass@result.aliyuncs.com/transcript.json",
		"https://result.aliyuncs.com:444/transcript.json",
	} {
		if _, err := client.DownloadResult(context.Background(), unsafeURL); err == nil {
			t.Errorf("DownloadResult(%q) unexpectedly succeeded", unsafeURL)
		}
	}
	if err := ValidateResult([]byte(`{"transcripts":[{"sentences":[{"begin_time":35,"end_time":34,"text":"bad"}]}]}`)); err == nil {
		t.Fatal("ValidateResult() accepted an invalid sentence interval")
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
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
