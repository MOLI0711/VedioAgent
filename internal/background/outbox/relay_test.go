package outbox

import (
	"testing"
	"time"
)

func TestRetryDelayBounds(t *testing.T) {
	for _, attempt := range []int{-1, 0, 1, 2, 4, 7, 100} {
		delay := retryDelay(attempt)
		if delay < 100*time.Millisecond || delay > time.Minute {
			t.Fatalf("retryDelay(%d) = %s, outside [100ms, 60s]", attempt, delay)
		}
	}
}
