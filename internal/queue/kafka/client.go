package kafka

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	ParseRequestedTopic = "mirrorcore.parse.requested.v1"
	ParseRejectedTopic  = "mirrorcore.parse.rejected.v1"
	ParseConsumerGroup  = "mirrorcore-parse-admission-v1"
)

func NewProducer(brokers []string) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.RecordRetries(5),
	)
}

func EnsureTopics(ctx context.Context, brokers []string) error {
	if len(brokers) == 0 {
		return errors.New("at least one Kafka broker is required")
	}
	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return err
	}
	defer client.Close()
	admin := kadm.NewClient(client)
	configs := map[string]*string{"retention.ms": kadm.StringPtr("604800000")}
	for _, topic := range []string{ParseRequestedTopic, ParseRejectedTopic} {
		if _, err := admin.CreateTopic(ctx, 1, 1, configs, topic); err != nil && err != kerr.TopicAlreadyExists {
			return err
		}
	}
	return nil
}

func NewConsumer(brokers []string, group, topic string) (*kgo.Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("at least one Kafka broker is required")
	}
	if group == "" || topic == "" {
		return nil, errors.New("Kafka consumer group and topic are required")
	}
	return kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
		kgo.BlockRebalanceOnPoll(),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.SessionTimeout(45*time.Second),
		kgo.RebalanceTimeout(60*time.Second),
	)
}
