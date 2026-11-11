package inbox

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"mirrorcore/internal/queue/kafka"
)

func TestValidateRecord(t *testing.T) {
	event := map[string]any{
		"event_id": uuid.NewString(), "event_type": "parse.requested", "schema_version": 1,
		"task_id": uuid.NewString(), "video_id": "11111111-1111-4111-8111-111111111111",
		"task_version": 4, "occurred_at": time.Now().UTC().Format(time.RFC3339Nano), "trace_id": uuid.NewString(),
	}
	value, err := json.Marshal(event)
	if err != nil {
		t.Fatal(err)
	}
	record := &kgo.Record{Topic: kafka.ParseRequestedTopic, Value: value}
	parsed, reason := validateRecord(record)
	if reason != "" || parsed.TaskVersion != 4 {
		t.Fatalf("valid event: reason=%q event=%+v", reason, parsed)
	}

	record.Value = []byte(`{"unknown":"field"}`)
	if _, reason := validateRecord(record); reason != "INVALID_JSON" {
		t.Fatalf("unknown field reason = %q, want INVALID_JSON", reason)
	}
	record.Value = make([]byte, 16*1024+1)
	if _, reason := validateRecord(record); reason != "INVALID_PAYLOAD_SIZE" {
		t.Fatalf("oversized payload reason = %q, want INVALID_PAYLOAD_SIZE", reason)
	}
}
