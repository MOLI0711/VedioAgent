package inbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"mirrorcore/internal/queue/kafka"
)

type Consumer struct {
	brokers   []string
	group     string
	topic     string
	processor *Processor
	logger    *slog.Logger
}

func NewConsumer(brokers []string, processor *Processor, logger *slog.Logger) *Consumer {
	if logger == nil {
		logger = slog.Default()
	}
	return &Consumer{brokers: brokers, group: kafka.ParseConsumerGroup, topic: kafka.ParseRequestedTopic, processor: processor, logger: logger}
}

func (c *Consumer) Run(ctx context.Context) error {
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return nil
		}
		client, err := kafka.NewConsumer(c.brokers, c.group, c.topic)
		if err == nil {
			err = c.consume(ctx, client)
			// BlockRebalanceOnPoll requires rebalances to be unblocked before
			// leaving the group; otherwise Close can wait for a revoke callback
			// that is itself waiting for AllowRebalance.
			client.CloseAllowingRebalance()
		}
		if ctx.Err() != nil {
			return nil
		}
		c.logger.Error("Kafka inbox consumer restarting", "error", err, "retry_in", backoff)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (c *Consumer) consume(ctx context.Context, client *kgo.Client) error {
	for {
		fetches := client.PollRecords(ctx, 1)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		records := fetches.Records()
		if len(records) > 0 {
			if err := c.processAndCommit(ctx, client, records[0]); err != nil {
				return err
			}
			continue
		}
		fetchErrors := fetches.Errors()
		if len(fetchErrors) > 0 {
			for _, fetchErr := range fetchErrors {
				c.logger.Error("Kafka poll failed", "topic", fetchErr.Topic, "partition", fetchErr.Partition, "error", fetchErr.Err)
			}
			return fmt.Errorf("poll Kafka: %w", fetchErrors[0].Err)
		}
	}
}

func (c *Consumer) processAndCommit(ctx context.Context, client *kgo.Client, record *kgo.Record) error {
	defer client.AllowRebalance()
	processCtx, processCancel := context.WithTimeout(ctx, 5*time.Second)
	err := c.processor.ProcessRecord(processCtx, record)
	processCancel()
	if err != nil {
		return fmt.Errorf("persist Kafka record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	commitCtx, commitCancel := context.WithTimeout(ctx, 3*time.Second)
	err = client.CommitRecords(commitCtx, record)
	commitCancel()
	if err != nil {
		return fmt.Errorf("commit Kafka offset %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	return nil
}
