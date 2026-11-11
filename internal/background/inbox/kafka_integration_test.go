package inbox_test

import (
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"mirrorcore/internal/application"
	"mirrorcore/internal/background/inbox"
	"mirrorcore/internal/background/outbox"
	"mirrorcore/internal/database"
	kafkaclient "mirrorcore/internal/queue/kafka"
	"mirrorcore/internal/repository/postgres"
)

func TestOutboxKafkaInboxOffsetHandoff(t *testing.T) {
	databaseURL := os.Getenv("TEST_DATABASE_URL")
	brokers := splitBrokers(os.Getenv("TEST_KAFKA_BROKERS"))
	if databaseURL == "" || len(brokers) == 0 {
		t.Skip("set TEST_DATABASE_URL and TEST_KAFKA_BROKERS to run Kafka/PostgreSQL integration test")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	pool := openIsolatedKafkaDatabase(t, ctx, databaseURL)
	var err error

	ownerID, videoID := uuid.New(), uuid.New()
	_, err = pool.Exec(ctx, `INSERT INTO users(id,email,password_hash) VALUES ($1,$2,'integration-test')`, ownerID, ownerID.String()+"@example.test")
	if err != nil {
		t.Fatalf("insert test user: %v", err)
	}
	_, err = pool.Exec(ctx, `INSERT INTO videos(id,owner_id,file_state,source_name,source_size_bytes)
		VALUES ($1,$2,'VALID','kafka-integration.mp4',1024)`, videoID, ownerID)
	if err != nil {
		_, _ = pool.Exec(ctx, `DELETE FROM users WHERE id=$1`, ownerID)
		t.Fatalf("insert test video: %v", err)
	}
	type rejectedOffset struct {
		topic     string
		partition int32
		offset    int64
		dlqID     uuid.UUID
	}
	var rejected []rejectedOffset
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		for _, item := range rejected {
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM kafka_rejections WHERE topic=$1 AND partition=$2 AND offset_id=$3`, item.topic, item.partition, item.offset)
			_, _ = pool.Exec(cleanupCtx, `DELETE FROM outbox_events WHERE event_id=$1`, item.dlqID)
		}
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM inbox_events WHERE task_id IN (SELECT id FROM parse_tasks WHERE owner_id=$1)`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM outbox_events WHERE aggregate_id IN (SELECT id FROM parse_tasks WHERE owner_id=$1)`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `UPDATE videos SET latest_task_id=NULL WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM parse_tasks WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM videos WHERE owner_id=$1`, ownerID)
		_, _ = pool.Exec(cleanupCtx, `DELETE FROM users WHERE id=$1`, ownerID)
	})

	service := application.NewResourceService(postgres.NewResourceRepository(pool))
	task, _, err := service.CreateParseTask(ctx, application.CreateParseTaskInput{
		VideoID: videoID, OwnerID: ownerID, RequestKey: "kafka-integration", TraceID: uuid.NewString(), ProfileID: "voice-v1",
	})
	if err != nil {
		t.Fatalf("create parse task: %v", err)
	}
	var eventID uuid.UUID
	var topic, key string
	var payload []byte
	if err := pool.QueryRow(ctx, `SELECT event_id,topic,event_key,payload FROM outbox_events WHERE aggregate_id=$1`, task.ID).
		Scan(&eventID, &topic, &key, &payload); err != nil {
		t.Fatalf("load outbox event: %v", err)
	}
	producer, err := kafkaclient.NewProducer(brokers)
	if err != nil {
		t.Fatalf("create Kafka producer: %v", err)
	}
	t.Cleanup(producer.Close)
	if err := kafkaclient.EnsureTopics(ctx, brokers); err != nil {
		t.Fatalf("ensure Kafka topics: %v", err)
	}
	relay := outbox.New(pool, outbox.NewKgoSender(producer), nil)
	if err := relay.PumpEvent(ctx, eventID); err != nil {
		t.Fatalf("publish outbox event: %v", err)
	}

	consumer, err := kafkaclient.NewConsumer(brokers, kafkaclient.ParseConsumerGroup, kafkaclient.ParseRequestedTopic)
	if err != nil {
		t.Fatalf("create test Kafka consumer: %v", err)
	}
	t.Cleanup(consumer.Close)
	processor := inbox.NewProcessor(pool)
	foundTarget := false
	processedCount := 0
	for !foundTarget {
		fetches := consumer.PollRecords(ctx, 1)
		records := fetches.Records()
		if len(records) != 1 {
			consumer.AllowRebalance()
			t.Fatalf("poll %d returned %d records, errors=%v", processedCount+1, len(records), fetches.Errors())
		}
		processedCount++
		record := records[0]
		if err := processor.ProcessRecord(ctx, record); err != nil {
			consumer.AllowRebalance()
			t.Fatalf("persist record %s[%d]@%d: %v", record.Topic, record.Partition, record.Offset, err)
		}
		var decoded struct {
			EventID uuid.UUID `json:"event_id"`
		}
		_ = json.Unmarshal(record.Value, &decoded)
		if decoded.EventID == eventID {
			if err := processor.ProcessRecord(ctx, record); err != nil {
				consumer.AllowRebalance()
				t.Fatalf("replay target Inbox record: %v", err)
			}
			foundTarget = true
		}
		if err := consumer.CommitRecords(ctx, record); err != nil {
			consumer.AllowRebalance()
			t.Fatalf("commit record %s[%d]@%d: %v", record.Topic, record.Partition, record.Offset, err)
		}
		consumer.AllowRebalance()
		var dlqID uuid.UUID
		if err := pool.QueryRow(ctx, `SELECT dlq_event_id FROM kafka_rejections WHERE topic=$1 AND partition=$2 AND offset_id=$3`,
			record.Topic, record.Partition, record.Offset).Scan(&dlqID); err == nil {
			rejected = append(rejected, rejectedOffset{topic: record.Topic, partition: record.Partition, offset: record.Offset, dlqID: dlqID})
		}
	}

	var status string
	var inboxCount int
	if err := pool.QueryRow(ctx, `SELECT status FROM parse_tasks WHERE id=$1`, task.ID).Scan(&status); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM inbox_events WHERE consumer_name='parse-admission-v1' AND event_id=$1`, eventID).Scan(&inboxCount); err != nil {
		t.Fatal(err)
	}
	var outboxStatus string
	if err := pool.QueryRow(ctx, `SELECT status FROM outbox_events WHERE event_id=$1`, eventID).Scan(&outboxStatus); err != nil {
		t.Fatal(err)
	}
	if status != "READY" || inboxCount != 1 || outboxStatus != "SENT" {
		t.Fatalf("task=%s inbox_count=%d outbox=%s, want READY/1/SENT", status, inboxCount, outboxStatus)
	}
}

func openIsolatedKafkaDatabase(t *testing.T, ctx context.Context, databaseURL string) *pgxpool.Pool {
	t.Helper()
	adminPool, err := database.Open(ctx, databaseURL)
	if err != nil {
		t.Fatalf("open integration database: %v", err)
	}
	t.Cleanup(adminPool.Close)

	schemaName := "kafka_handoff_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	if _, err := adminPool.Exec(ctx, "CREATE SCHEMA "+pgx.Identifier{schemaName}.Sanitize()); err != nil {
		t.Fatalf("create isolated Kafka integration schema: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = adminPool.Exec(cleanupCtx, "DROP SCHEMA "+pgx.Identifier{schemaName}.Sanitize()+" CASCADE")
	})

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		t.Fatalf("parse integration database URL: %v", err)
	}
	config.ConnConfig.RuntimeParams["search_path"] = schemaName + ",public"
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		t.Fatalf("open isolated Kafka integration pool: %v", err)
	}
	t.Cleanup(pool.Close)
	if err := database.Migrate(ctx, pool); err != nil {
		t.Fatalf("migrate isolated Kafka schema: %v", err)
	}
	return pool
}

func splitBrokers(value string) []string {
	var brokers []string
	for _, broker := range strings.Split(value, ",") {
		if broker = strings.TrimSpace(broker); broker != "" {
			brokers = append(brokers, broker)
		}
	}
	return brokers
}
