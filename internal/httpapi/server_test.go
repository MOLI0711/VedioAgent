package httpapi

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/google/uuid"
)

func TestHealthIncludesRequestID(t *testing.T) {
	router := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	router.ServeHTTP(recorder, request)

	if recorder.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", recorder.Code, http.StatusOK)
	}
	var response struct {
		Data struct {
			Status string `json:"status"`
		} `json:"data"`
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if response.Data.Status != "ok" {
		t.Fatalf("status field = %q, want ok", response.Data.Status)
	}
	if _, err := uuid.Parse(response.RequestID); err != nil {
		t.Fatalf("request_id %q is not a UUID: %v", response.RequestID, err)
	}
}

func TestHealthPreservesValidRequestID(t *testing.T) {
	router := New(nil, slog.New(slog.NewTextHandler(io.Discard, nil)), false)
	wantID := "36c2b559-4f09-4ef3-b4a3-7c099027f50c"
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	request.Header.Set("X-Request-ID", wantID)
	router.ServeHTTP(recorder, request)
	if got := recorder.Header().Get("X-Request-ID"); got != wantID {
		t.Fatalf("X-Request-ID = %q, want %q", got, wantID)
	}
}
