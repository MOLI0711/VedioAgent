package observability

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

const queueSnapshotSQL = `
WITH snapshots AS (
	SELECT 'parse_task'::text AS queue, status AS state, count(*)::float8 AS items,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (
			statement_timestamp() - CASE
				WHEN status = 'WAIT_DISPATCH' THEN created_at
				WHEN status IN ('READY','WAITING','RETRY_WAIT') THEN next_run_at
				WHEN status = 'RUNNING' AND lease_until <= statement_timestamp() THEN lease_until
				ELSE statement_timestamp()
			END))), 0), 0)::float8 AS oldest_age,
		count(*) FILTER (WHERE status = 'RUNNING' AND lease_until <= statement_timestamp())::float8 AS expired_leases,
		0::float8 AS overdue_deadlines
	FROM parse_tasks
	WHERE status IN ('WAIT_DISPATCH','READY','RUNNING','WAITING','RETRY_WAIT')
	GROUP BY status
	UNION ALL
	SELECT 'outbox', status, count(*)::float8,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (
			statement_timestamp() - CASE
				WHEN status = 'PENDING' THEN next_send_at
				WHEN status = 'SENDING' AND lease_until <= statement_timestamp() THEN lease_until
				WHEN status = 'FAILED' THEN created_at
				ELSE statement_timestamp()
			END))), 0), 0)::float8,
		count(*) FILTER (WHERE status = 'SENDING' AND lease_until <= statement_timestamp())::float8, 0::float8
	FROM outbox_events WHERE status IN ('PENDING','SENDING','FAILED') GROUP BY status
	UNION ALL
	SELECT 'kafka_rejection','REJECTED',count(*)::float8,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (statement_timestamp()-created_at))),0),0)::float8,
		0::float8,0::float8
	FROM kafka_rejections
	UNION ALL
	SELECT 'content_job', state, count(*)::float8,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (
			statement_timestamp() - CASE
				WHEN state IN ('READY','RETRY_WAIT') THEN next_run_at
				WHEN state = 'RUNNING' AND lease_until <= statement_timestamp() THEN lease_until
				WHEN state = 'FAILED' THEN created_at
				ELSE statement_timestamp()
			END))), 0), 0)::float8,
		count(*) FILTER (WHERE state = 'RUNNING' AND lease_until <= statement_timestamp())::float8, 0::float8
	FROM content_jobs WHERE state IN ('READY','RUNNING','RETRY_WAIT','FAILED') GROUP BY state
	UNION ALL
	SELECT 'video_deletion', state, count(*)::float8,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (
			statement_timestamp() - CASE
				WHEN state IN ('PENDING','RETRY_WAIT') THEN next_run_at
				WHEN state = 'RUNNING' AND lease_until <= statement_timestamp() THEN lease_until
				ELSE statement_timestamp()
			END))), 0), 0)::float8,
		count(*) FILTER (WHERE state = 'RUNNING' AND lease_until <= statement_timestamp())::float8, 0::float8
	FROM video_deletion_jobs WHERE state IN ('PENDING','RUNNING','RETRY_WAIT') GROUP BY state
	UNION ALL
	SELECT 'asr_remote',
		CASE WHEN state = 'UNKNOWN' AND provider_task_id IS NULL THEN 'unknown_no_task_id'
			WHEN state = 'UNKNOWN' THEN 'unknown_with_task_id' ELSE lower(state) END,
		count(*)::float8,
		GREATEST(COALESCE(MAX(EXTRACT(EPOCH FROM (
			statement_timestamp() - CASE
				WHEN state = 'UNKNOWN' THEN COALESCE(unknown_since, updated_at)
				WHEN state IN ('ACCEPTED','RUNNING') THEN next_poll_at
				WHEN state = 'RESULT_READY' THEN updated_at
				ELSE prepared_at
			END))), 0), 0)::float8,
		0::float8,
		count(*) FILTER (WHERE state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING') AND provider_deadline_at <= statement_timestamp())::float8
	FROM asr_submissions
	WHERE state IN ('SUBMITTING','UNKNOWN','ACCEPTED','RUNNING','RESULT_READY')
	GROUP BY 2
)
SELECT queue, state, items, oldest_age, expired_leases, overdue_deadlines FROM snapshots
ORDER BY queue, state`

type queueCollector struct {
	service  string
	pool     *pgxpool.Pool
	items    *prometheus.Desc
	oldest   *prometheus.Desc
	expired  *prometheus.Desc
	err      *prometheus.Desc
	duration *prometheus.Desc
	overdue  *prometheus.Desc
}

func newQueueCollector(service string, pool *pgxpool.Pool) *queueCollector {
	return &queueCollector{
		service:  service,
		pool:     pool,
		items:    prometheus.NewDesc("mirrorcore_db_queue_items", "Durable work items by queue and state.", []string{"service", "queue", "state"}, nil),
		oldest:   prometheus.NewDesc("mirrorcore_db_queue_oldest_age_seconds", "Age of the oldest due or expired work item in a queue state.", []string{"service", "queue", "state"}, nil),
		expired:  prometheus.NewDesc("mirrorcore_db_queue_expired_leases", "Work items whose database lease has expired.", []string{"service", "queue", "state"}, nil),
		overdue:  prometheus.NewDesc("mirrorcore_db_queue_overdue_deadlines", "Remote provider jobs whose persisted deadline has expired.", []string{"service", "queue", "state"}, nil),
		err:      prometheus.NewDesc("mirrorcore_db_queue_scrape_error", "Whether the durable queue snapshot query failed.", []string{"service"}, nil),
		duration: prometheus.NewDesc("mirrorcore_db_queue_scrape_duration_seconds", "Duration of the durable queue snapshot query.", []string{"service"}, nil),
	}
}

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.items
	ch <- c.oldest
	ch <- c.expired
	ch <- c.overdue
	ch <- c.err
	ch <- c.duration
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	started := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	rows, err := c.pool.Query(ctx, queueSnapshotSQL)
	if err != nil {
		ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 1, c.service)
		ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, time.Since(started).Seconds(), c.service)
		return
	}
	defer rows.Close()
	for rows.Next() {
		var queue, state string
		var items, oldestAge, expiredLeases, overdueDeadlines float64
		if err := rows.Scan(&queue, &state, &items, &oldestAge, &expiredLeases, &overdueDeadlines); err != nil {
			ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 1, c.service)
			ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, time.Since(started).Seconds(), c.service)
			return
		}
		ch <- prometheus.MustNewConstMetric(c.items, prometheus.GaugeValue, items, c.service, queue, state)
		ch <- prometheus.MustNewConstMetric(c.oldest, prometheus.GaugeValue, oldestAge, c.service, queue, state)
		ch <- prometheus.MustNewConstMetric(c.expired, prometheus.GaugeValue, expiredLeases, c.service, queue, state)
		ch <- prometheus.MustNewConstMetric(c.overdue, prometheus.GaugeValue, overdueDeadlines, c.service, queue, state)
	}
	if err := rows.Err(); err != nil {
		ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 1, c.service)
	} else {
		ch <- prometheus.MustNewConstMetric(c.err, prometheus.GaugeValue, 0, c.service)
	}
	ch <- prometheus.MustNewConstMetric(c.duration, prometheus.GaugeValue, time.Since(started).Seconds(), c.service)
}
