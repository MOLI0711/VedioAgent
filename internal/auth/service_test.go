package auth

import "testing"

func TestVerifyCSRF(t *testing.T) {
	token, hash, err := newToken()
	if err != nil {
		t.Fatalf("newToken() error = %v", err)
	}
	if !VerifyCSRF(hash[:], token) {
		t.Fatal("expected generated token to match its stored hash")
	}
	if VerifyCSRF(hash[:], token+"x") {
		t.Fatal("expected modified token to fail")
	}
	if VerifyCSRF(hash[:], "") {
		t.Fatal("expected empty token to fail")
	}
}

func TestNewTokenIsOpaqueAndUnique(t *testing.T) {
	token1, hash1, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	token2, hash2, err := newToken()
	if err != nil {
		t.Fatal(err)
	}
	if token1 == token2 || hash1 == hash2 {
		t.Fatal("session tokens must be unique")
	}
	if len(token1) < 40 {
		t.Fatalf("token length = %d, want at least 40 URL-safe characters", len(token1))
	}
}
