package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/google/uuid"
	"mirrorcore/internal/auth"
	"mirrorcore/internal/database"
)

func TestSessionCookieCSRFAndRevocation(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	if databaseURL == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	pool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate integration database: %v", err)
	}
	email := uuid.NewString() + "@example.test"
	password := "integration-password-123"
	service := auth.NewService(pool)
	if err := service.SeedDemoUser(ctx, email, password); err != nil {
		t.Fatalf("seed test user: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE email=$1`, email)
	})

	router := New(pool, nil, false)
	loginBody, _ := json.Marshal(map[string]string{"email": email, "password": password})
	login := httptest.NewRecorder()
	router.ServeHTTP(login, httptest.NewRequest(http.MethodPost, "/v1/sessions", bytes.NewReader(loginBody)))
	if login.Code != http.StatusCreated {
		t.Fatalf("login status = %d, body=%s", login.Code, login.Body.String())
	}
	cookies := login.Result().Cookies()
	if len(cookies) != 1 || cookies[0].Name != auth.SessionCookieName || !cookies[0].HttpOnly || cookies[0].SameSite != http.SameSiteStrictMode {
		t.Fatalf("unexpected session cookie attributes: %+v", cookies)
	}
	var loginResponse struct {
		Data struct {
			CSRFToken string `json:"csrf_token"`
		} `json:"data"`
	}
	if err := json.Unmarshal(login.Body.Bytes(), &loginResponse); err != nil {
		t.Fatalf("decode login response: %v", err)
	}
	if loginResponse.Data.CSRFToken == "" {
		t.Fatal("login response omitted csrf_token")
	}

	request := httptest.NewRequest(http.MethodPost, "/v1/videos/00000000-0000-0000-0000-000000000001/parse-tasks", bytes.NewReader([]byte(`{}`)))
	request.AddCookie(cookies[0])
	missingCSRF := httptest.NewRecorder()
	router.ServeHTTP(missingCSRF, request)
	if missingCSRF.Code != http.StatusForbidden {
		t.Fatalf("write without csrf status = %d, want %d", missingCSRF.Code, http.StatusForbidden)
	}

	logoutRequest := httptest.NewRequest(http.MethodDelete, "/v1/session", nil)
	logoutRequest.AddCookie(cookies[0])
	logoutRequest.Header.Set("X-CSRF-Token", loginResponse.Data.CSRFToken)
	logout := httptest.NewRecorder()
	router.ServeHTTP(logout, logoutRequest)
	if logout.Code != http.StatusOK {
		t.Fatalf("logout status = %d, body=%s", logout.Code, logout.Body.String())
	}

	repeatLogout := httptest.NewRecorder()
	router.ServeHTTP(repeatLogout, httptest.NewRequest(http.MethodDelete, "/v1/session", nil))
	if repeatLogout.Code != http.StatusOK {
		t.Fatalf("repeat logout status = %d, want %d", repeatLogout.Code, http.StatusOK)
	}

	protected := httptest.NewRequest(http.MethodGet, "/v1/videos", nil)
	protected.AddCookie(cookies[0])
	denied := httptest.NewRecorder()
	router.ServeHTTP(denied, protected)
	if denied.Code != http.StatusUnauthorized {
		t.Fatalf("request after logout status = %d, want %d", denied.Code, http.StatusUnauthorized)
	}
}
