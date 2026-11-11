package application

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestCursorRoundTrip(t *testing.T) {
	wantTime := time.Date(2026, 11, 10, 12, 34, 56, 123456789, time.FixedZone("UTC+8", 8*60*60))
	wantID := uuid.New()
	encoded := EncodeCursor(wantTime, wantID)
	gotTime, gotID, err := ParseCursor(encoded)
	if err != nil {
		t.Fatalf("ParseCursor() error = %v", err)
	}
	if !gotTime.Equal(wantTime) || gotID != wantID {
		t.Fatalf("round trip = (%s, %s), want (%s, %s)", gotTime, gotID, wantTime, wantID)
	}
}

func TestParseCursorRejectsMalformedInput(t *testing.T) {
	for _, value := range []string{"!", "", "not-a-time|00000000-0000-0000-0000-000000000000"} {
		if _, _, err := ParseCursor(value); err == nil {
			t.Fatalf("ParseCursor(%q) unexpectedly succeeded", value)
		}
	}
}
