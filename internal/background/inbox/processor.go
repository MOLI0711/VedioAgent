package inbox

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"
	"mirrorcore/internal/queue/kafka"
)

const consumerName = "parse-admission-v1"

type Processor struct{ pool *pgxpool.Pool }

func NewProcessor(pool *pgxpool.Pool) *Processor { return &Processor{pool: pool} }

type parseEvent struct {
	EventID       uuid.UUID `json:"event_id"`
	EventType     string    `json:"event_type"`
	SchemaVersion int       `json:"schema_version"`
	TaskID        uuid.UUID `json:"task_id"`
	VideoID       uuid.UUID `json:"video_id"`
	TaskVersion   int32     `json:"task_version"`
	OccurredAt    string    `json:"occurred_at"`
	TraceID       string    `json:"trace_id"`
}

// ProcessRecord persists either Inbox acceptance or a durable rejection before
// the caller commits the Kafka offset.
func (p *Processor) ProcessRecord(ctx context.Context, record *kgo.Record) error {
	payloadHash := sha256.Sum256(record.Value)
	tx, err := p.pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted})
	if err != nil {
		return fmt.Errorf("begin inbox transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var existingRejectionHash []byte
	err = tx.QueryRow(ctx, `SELECT payload_hash FROM kafka_rejections WHERE topic=$1 AND partition=$2 AND offset_id=$3`,
		record.Topic, record.Partition, record.Offset).Scan(&existingRejectionHash)
	if err == nil {
		if !bytes.Equal(existingRejectionHash, payloadHash[:]) {
			return errors.New("Kafka offset already rejected with a different payload")
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check existing Kafka rejection: %w", err)
	}

	event, permanentReason := validateRecord(record)
	if permanentReason == "" && string(record.Key) != event.VideoID.String() {
		permanentReason = "KAFKA_KEY_MISMATCH"
	}
	if permanentReason != "" {
		if err := p.reject(ctx, tx, record, payloadHash[:], permanentReason, uuid.Nil); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	var existingInboxHash []byte
	err = tx.QueryRow(ctx, `SELECT payload_hash FROM inbox_events WHERE consumer_name=$1 AND event_id=$2`, consumerName, event.EventID).
		Scan(&existingInboxHash)
	if err == nil {
		if !bytes.Equal(existingInboxHash, payloadHash[:]) {
			if err := p.reject(ctx, tx, record, payloadHash[:], "EVENT_ID_PAYLOAD_MISMATCH", event.EventID); err != nil {
				return err
			}
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check inbox event identity: %w", err)
	}

	var existingOffsetHash []byte
	var existingOffsetEventID uuid.UUID
	err = tx.QueryRow(ctx, `SELECT payload_hash, event_id FROM inbox_events WHERE topic=$1 AND partition=$2 AND offset_id=$3`,
		record.Topic, record.Partition, record.Offset).Scan(&existingOffsetHash, &existingOffsetEventID)
	if err == nil {
		if bytes.Equal(existingOffsetHash, payloadHash[:]) && existingOffsetEventID == event.EventID {
			return tx.Commit(ctx)
		}
		if err := p.reject(ctx, tx, record, payloadHash[:], "KAFKA_OFFSET_IDENTITY_MISMATCH", event.EventID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return fmt.Errorf("check inbox offset identity: %w", err)
	}

	var taskStatus string
	err = tx.QueryRow(ctx, `SELECT status FROM parse_tasks
		WHERE id=$1 AND video_id=$2 AND task_version=$3 FOR UPDATE`, event.TaskID, event.VideoID, event.TaskVersion).Scan(&taskStatus)
	if errors.Is(err, pgx.ErrNoRows) {
		if err := p.reject(ctx, tx, record, payloadHash[:], "TASK_IDENTITY_NOT_FOUND", event.EventID); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}
	if err != nil {
		return fmt.Errorf("load task for Kafka event: %w", err)
	}

	_, err = tx.Exec(ctx, `INSERT INTO inbox_events(consumer_name,event_id,payload_hash,task_id,topic,partition,offset_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7)`, consumerName, event.EventID, payloadHash[:], event.TaskID,
		record.Topic, record.Partition, record.Offset)
	if err != nil {
		return fmt.Errorf("insert inbox event: %w", err)
	}
	if taskStatus == "WAIT_DISPATCH" {
		if _, err := tx.Exec(ctx, `UPDATE parse_tasks SET status='READY', admitted_at=clock_timestamp(),
			deadline_at=clock_timestamp()+interval '6 hours', next_run_at=clock_timestamp()
			WHERE id=$1 AND status='WAIT_DISPATCH'`, event.TaskID); err != nil {
			return fmt.Errorf("mark task admitted: %w", err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit inbox event: %w", err)
	}
	return nil
}

func validateRecord(record *kgo.Record) (parseEvent, string) {
	if record.Topic != kafka.ParseRequestedTopic {
		return parseEvent{}, "UNEXPECTED_TOPIC"
	}
	if len(record.Value) == 0 || len(record.Value) > 16*1024 {
		return parseEvent{}, "INVALID_PAYLOAD_SIZE"
	}
	var event parseEvent
	decoder := json.NewDecoder(bytes.NewReader(record.Value))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&event); err != nil {
		return parseEvent{}, "INVALID_JSON"
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return parseEvent{}, "MULTIPLE_JSON_VALUES"
	}
	if event.EventID == uuid.Nil || event.TaskID == uuid.Nil || event.VideoID == uuid.Nil || event.TaskVersion <= 0 {
		return parseEvent{}, "INVALID_EVENT_IDENTITY"
	}
	if event.EventType != "parse.requested" || event.SchemaVersion != 1 {
		return parseEvent{}, "UNSUPPORTED_EVENT_SCHEMA"
	}
	if _, err := time.Parse(time.RFC3339Nano, event.OccurredAt); err != nil {
		return parseEvent{}, "INVALID_EVENT_TIME"
	}
	if _, err := uuid.Parse(event.TraceID); err != nil {
		return parseEvent{}, "INVALID_TRACE_ID"
	}
	return event, ""
}

func (p *Processor) reject(ctx context.Context, tx pgx.Tx, record *kgo.Record, payloadHash []byte, reason string, sourceEventID uuid.UUID) error {
	dlqEventID := uuid.New()
	key := fmt.Sprintf("%s:%d:%d", record.Topic, record.Partition, record.Offset)
	payload, err := json.Marshal(map[string]any{
		"event_id": dlqEventID, "event_type": "parse.rejected", "schema_version": 1,
		"source_topic": record.Topic, "source_partition": record.Partition, "source_offset": record.Offset,
		"source_event_id": nullableUUID(sourceEventID), "payload_sha256": hex.EncodeToString(payloadHash), "reason": reason,
	})
	if err != nil {
		return fmt.Errorf("encode dead-letter event: %w", err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO outbox_events(
		event_id,aggregate_id,event_type,schema_version,topic,event_key,payload,status
	) VALUES ($1,$2,'parse.rejected',1,$3,$4,$5,'PENDING')`,
		dlqEventID, dlqEventID, kafka.ParseRejectedTopic, key, payload); err != nil {
		return fmt.Errorf("insert dead-letter outbox: %w", err)
	}
	prefix := record.Value
	if len(prefix) > 512 {
		prefix = prefix[:512]
	}
	if _, err := tx.Exec(ctx, `INSERT INTO kafka_rejections(
		topic,partition,offset_id,payload_hash,payload_prefix,reason,dlq_event_id
	) VALUES ($1,$2,$3,$4,$5,$6,$7)`, record.Topic, record.Partition, record.Offset, payloadHash, prefix, reason, dlqEventID); err != nil {
		return fmt.Errorf("insert Kafka rejection: %w", err)
	}
	return nil
}

func nullableUUID(id uuid.UUID) any {
	if id == uuid.Nil {
		return nil
	}
	return id
}
