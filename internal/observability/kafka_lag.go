package observability

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type KafkaPartitionLag struct {
	Topic             string
	Partition         int32
	Records           int64
	OffsetBehindStart bool
}

type KafkaConsumerLagSource interface {
	Snapshot(context.Context) ([]KafkaPartitionLag, error)
	Group() string
	Topic() string
}

type KafkaConsumerLagReader struct {
	admin *kadm.Client
	group string
	topic string
}

func NewKafkaConsumerLagReader(client *kgo.Client, group, topic string) (*KafkaConsumerLagReader, error) {
	if client == nil || group == "" || topic == "" {
		return nil, errors.New("Kafka lag reader requires client, consumer group, and topic")
	}
	return &KafkaConsumerLagReader{admin: kadm.NewClient(client), group: group, topic: topic}, nil
}

func (r *KafkaConsumerLagReader) Group() string { return r.group }

func (r *KafkaConsumerLagReader) Topic() string { return r.topic }

func (r *KafkaConsumerLagReader) Snapshot(ctx context.Context) ([]KafkaPartitionLag, error) {
	starts, err := r.admin.ListStartOffsets(ctx, r.topic)
	if err != nil {
		return nil, fmt.Errorf("list Kafka start offsets: %w", err)
	}
	if err := starts.Error(); err != nil {
		return nil, fmt.Errorf("read Kafka start offsets: %w", err)
	}
	ends, err := r.admin.ListEndOffsets(ctx, r.topic)
	if err != nil {
		return nil, fmt.Errorf("list Kafka end offsets: %w", err)
	}
	if err := ends.Error(); err != nil {
		return nil, fmt.Errorf("read Kafka end offsets: %w", err)
	}
	committed, err := r.admin.FetchOffsets(ctx, r.group)
	if err != nil {
		return nil, fmt.Errorf("fetch Kafka consumer offsets: %w", err)
	}
	if err := committed.Error(); err != nil {
		return nil, fmt.Errorf("read Kafka consumer offsets: %w", err)
	}

	topicEnds, ok := ends[r.topic]
	if !ok || len(topicEnds) == 0 {
		return nil, fmt.Errorf("Kafka end offsets missing for topic %s", r.topic)
	}
	partitions := make([]KafkaPartitionLag, 0, len(topicEnds))
	for partition, end := range topicEnds {
		if partition < 0 {
			return nil, fmt.Errorf("Kafka end offset contains invalid partition %d for topic %s", partition, r.topic)
		}
		if end.Err != nil {
			return nil, fmt.Errorf("read Kafka end offset for %s[%d]: %w", r.topic, partition, end.Err)
		}
		if end.Offset < 0 {
			return nil, fmt.Errorf("Kafka end offset is invalid for %s[%d]", r.topic, partition)
		}
		start, ok := starts.Lookup(r.topic, partition)
		if !ok {
			return nil, fmt.Errorf("Kafka start offset missing for %s[%d]", r.topic, partition)
		}
		if start.Err != nil {
			return nil, fmt.Errorf("read Kafka start offset for %s[%d]: %w", r.topic, partition, start.Err)
		}
		if start.Offset < 0 {
			return nil, fmt.Errorf("Kafka start offset is invalid for %s[%d]", r.topic, partition)
		}
		nextOffset := start.Offset
		offsetBehindStart := false
		if groupOffset, ok := committed.Lookup(r.topic, partition); ok && groupOffset.Err == nil && groupOffset.At >= 0 {
			nextOffset = groupOffset.At
		}
		if nextOffset < start.Offset {
			offsetBehindStart = true
			nextOffset = start.Offset
		}
		if nextOffset > end.Offset {
			nextOffset = end.Offset
		}
		partitions = append(partitions, KafkaPartitionLag{Topic: r.topic, Partition: partition,
			Records: end.Offset - nextOffset, OffsetBehindStart: offsetBehindStart})
	}
	return partitions, nil
}

type kafkaConsumerLagCollector struct {
	service  string
	group    string
	topic    string
	source   KafkaConsumerLagSource
	lag      *prometheus.Desc
	behind   *prometheus.Desc
	err      *prometheus.Desc
	duration *prometheus.Desc
}

func newKafkaConsumerLagCollector(service string, source KafkaConsumerLagSource, group, topic string) *kafkaConsumerLagCollector {
	return &kafkaConsumerLagCollector{
		service:  service,
		group:    boundedLabel(group, []string{"mirrorcore-parse-admission-v1"}, "other"),
		topic:    boundedLabel(topic, []string{"mirrorcore.parse.requested.v1"}, "other"),
		source:   source,
		lag:      prometheus.NewDesc("mirrorcore_kafka_consumer_group_lag_records", "Readable Kafka records between the committed consumer offset or retained start offset and the partition end offset.", []string{"service", "group", "topic", "partition"}, nil),
		behind:   prometheus.NewDesc("mirrorcore_kafka_consumer_offset_behind_start", "Whether the committed consumer offset is older than the partition's retained start offset.", []string{"service", "group", "topic", "partition"}, nil),
		err:      prometheus.NewDesc("mirrorcore_kafka_lag_scrape_error", "Whether the Kafka consumer lag snapshot query failed.", []string{"service", "group"}, nil),
		duration: prometheus.NewDesc("mirrorcore_kafka_lag_scrape_duration_seconds", "Duration of the Kafka consumer lag snapshot query.", []string{"service", "group"}, nil),
	}
}

func (c *kafkaConsumerLagCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.lag
	ch <- c.behind
	ch <- c.err
	ch <- c.duration
}

func (c *kafkaConsumerLagCollector) Collect(ch chan<- prometheus.Metric) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	partitions, err := c.source.Snapshot(ctx)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 1, c.service, c.group)
		ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, time.Since(started).Seconds(), c.service, c.group)
		return
	}
	for _, partition := range partitions {
		labels := []string{c.service, c.group, c.topic, strconv.FormatInt(int64(partition.Partition), 10)}
		ch <- prometheus.MustNewConstMetric(c.lag, prometheus.GaugeValue, float64(max(partition.Records, 0)), labels...)
		behind := 0.0
		if partition.OffsetBehindStart {
			behind = 1
		}
		ch <- prometheus.MustNewConstMetric(c.behind, prometheus.GaugeValue, behind, labels...)
	}
	ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 0, c.service, c.group)
	ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, time.Since(started).Seconds(), c.service, c.group)
}
