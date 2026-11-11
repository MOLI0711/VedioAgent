package observability

import (
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	registry                        *prometheus.Registry
	service                         string
	poolOnce                        sync.Once
	queueOnce                       sync.Once
	kafkaLagOnce                    sync.Once
	httpRequests                    *prometheus.CounterVec
	httpDuration                    *prometheus.HistogramVec
	providerCalls                   *prometheus.CounterVec
	providerDuration                *prometheus.HistogramVec
	providerTokens                  *prometheus.CounterVec
	quotaReservedTokens             *prometheus.CounterVec
	providerLedgerPersistenceErrors *prometheus.CounterVec
	quotaPruneFailures              *prometheus.CounterVec
	quotaAdmissions                 *prometheus.CounterVec
	quotaWait                       *prometheus.HistogramVec
	workerRuns                      *prometheus.CounterVec
	workerDuration                  *prometheus.HistogramVec
	activeWorkers                   *prometheus.GaugeVec
}

func New(service string) *Metrics {
	service = boundedLabel(service, []string{"api", "background"}, "other")
	registry := prometheus.NewRegistry()
	m := &Metrics{
		registry: registry,
		service:  service,
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_http_requests_total", Help: "Completed API HTTP requests.",
		}, []string{"service", "method", "route", "status"}),
		httpDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mirrorcore_http_request_duration_seconds", Help: "API HTTP request duration in seconds.",
			Buckets: prometheus.DefBuckets,
		}, []string{"service", "method", "route"}),
		providerCalls: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_calls_total", Help: "Completed synchronous model provider calls.",
		}, []string{"service", "model", "outcome", "error_class"}),
		providerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mirrorcore_provider_call_duration_seconds", Help: "Synchronous model provider call duration in seconds.",
			Buckets: []float64{0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 20, 30, 60, 90},
		}, []string{"service", "model", "outcome"}),
		providerTokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_tokens_total", Help: "Provider token usage, split by actual usage and estimate fallback.",
		}, []string{"service", "model", "source"}),
		quotaReservedTokens: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_quota_tokens_reserved_total", Help: "Estimated provider tokens reserved before a model call.",
		}, []string{"service", "model"}),
		providerLedgerPersistenceErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_quota_persistence_failures_total", Help: "Provider quota ledger writes that failed after a remote call.",
		}, []string{"service", "model", "operation"}),
		quotaPruneFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_quota_prune_failures_total", Help: "Provider quota reservation pruning batches that failed.",
		}, []string{"service"}),
		quotaAdmissions: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_provider_quota_admissions_total", Help: "Provider quota reservation admission outcomes.",
		}, []string{"service", "model", "result"}),
		quotaWait: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mirrorcore_provider_quota_wait_seconds", Help: "Time spent waiting for a provider quota reservation.",
			Buckets: []float64{0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60},
		}, []string{"service", "model"}),
		workerRuns: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "mirrorcore_worker_stage_runs_total", Help: "Completed background worker stage attempts.",
		}, []string{"service", "worker", "stage", "outcome"}),
		workerDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "mirrorcore_worker_stage_duration_seconds", Help: "Background worker stage attempt duration in seconds.",
			Buckets: []float64{0.1, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600},
		}, []string{"service", "worker", "stage", "outcome"}),
		activeWorkers: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "mirrorcore_worker_active", Help: "Background worker jobs currently being processed.",
		}, []string{"service", "worker"}),
	}
	registry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		m.httpRequests, m.httpDuration, m.providerCalls, m.providerDuration,
		m.providerTokens, m.quotaReservedTokens, m.providerLedgerPersistenceErrors, m.quotaPruneFailures,
		m.quotaAdmissions, m.quotaWait,
		m.workerRuns, m.workerDuration, m.activeWorkers,
	)
	return m
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

func (m *Metrics) AttachPool(pool *pgxpool.Pool) {
	if m == nil || pool == nil {
		return
	}
	m.poolOnce.Do(func() {
		m.registry.MustRegister(newPGXPoolCollector(m.service, pool))
	})
}

// AttachQueueStats exports bounded aggregate snapshots from durable work tables.
// Keep it on the background process to avoid duplicate database-wide counts.
func (m *Metrics) AttachQueueStats(pool *pgxpool.Pool) {
	if m == nil || pool == nil {
		return
	}
	m.queueOnce.Do(func() {
		m.registry.MustRegister(newQueueCollector(m.service, pool))
	})
}

// AttachKafkaConsumerLag exports committed-offset lag for the configured
// consumer group and topic. Keep this on the background process only.
func (m *Metrics) AttachKafkaConsumerLag(source KafkaConsumerLagSource) {
	if m == nil || source == nil || source.Group() == "" || source.Topic() == "" {
		return
	}
	m.kafkaLagOnce.Do(func() {
		m.registry.MustRegister(newKafkaConsumerLagCollector(m.service, source, source.Group(), source.Topic()))
	})
}

func (m *Metrics) ObserveHTTPRequest(method, route string, status int, duration time.Duration) {
	if m == nil {
		return
	}
	method = boundedLabel(strings.ToUpper(method), []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"}, "OTHER")
	if route == "" || len(route) > 128 {
		route = "unmatched"
	}
	statusLabel := strconv.Itoa(status)
	if status < 100 || status > 599 {
		statusLabel = "other"
	}
	labels := []string{m.service, method, route}
	m.httpRequests.WithLabelValues(m.service, method, route, statusLabel).Inc()
	m.httpDuration.WithLabelValues(labels...).Observe(duration.Seconds())
}

func (m *Metrics) ObserveProviderCall(model, outcome, errorClass string, duration time.Duration) {
	if m == nil {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	outcome = boundedLabel(outcome, []string{"success", "failure"}, "other")
	errorClass = boundedLabel(errorClass,
		[]string{"none", "rate_limited", "client_error", "server_error", "transport_error", "provider_error", "canceled"}, "other")
	m.providerCalls.WithLabelValues(m.service, model, outcome, errorClass).Inc()
	m.providerDuration.WithLabelValues(m.service, model, outcome).Observe(duration.Seconds())
}

func (m *Metrics) ObserveProviderTokens(model, source string, tokens int64) {
	if m == nil || tokens <= 0 {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	source = boundedLabel(source, []string{"supplier_actual", "fallback_estimate"}, "other")
	m.providerTokens.WithLabelValues(m.service, model, source).Add(float64(tokens))
}

func (m *Metrics) ObserveProviderTokenReservation(model string, tokens int64) {
	if m == nil || tokens <= 0 {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	m.quotaReservedTokens.WithLabelValues(m.service, model).Add(float64(tokens))
}

func (m *Metrics) ObserveProviderLedgerPersistenceFailure(model, operation string) {
	if m == nil {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	operation = boundedLabel(operation, []string{"complete", "fail"}, "other")
	m.providerLedgerPersistenceErrors.WithLabelValues(m.service, model, operation).Inc()
}

func (m *Metrics) ObserveProviderQuotaPruneFailure() {
	if m == nil {
		return
	}
	m.quotaPruneFailures.WithLabelValues(m.service).Inc()
}

func (m *Metrics) ObserveQuotaAdmission(model, result string) {
	if m == nil {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	result = boundedLabel(result, []string{"reserved", "tpm_rejected", "canceled", "error"}, "other")
	m.quotaAdmissions.WithLabelValues(m.service, model, result).Inc()
}

func (m *Metrics) ObserveQuotaWait(model string, duration time.Duration) {
	if m == nil || duration <= 0 {
		return
	}
	model = boundedLabel(model, []string{"text-embedding-v4", "qwen3.7-plus-2026-05-26"}, "other")
	m.quotaWait.WithLabelValues(m.service, model).Observe(duration.Seconds())
}

func (m *Metrics) ObserveWorkerStage(worker, stage, outcome string, duration time.Duration) {
	if m == nil {
		return
	}
	worker = boundedLabel(worker, []string{"media", "content", "deletion"}, "other")
	stage = boundedLabel(stage, []string{
		"PROBE", "AUDIO", "ASR", "TRANSCRIPT", "CHUNK", "EMBEDDING", "PUBLISH",
		"WAIT_UPLOADS", "DISCOVER_OBJECTS", "DELETE_OBJECTS", "DELETE_OBJECTS_FINAL", "DISCOVER_OBJECTS_FINAL",
		"CLEAR_VIDEO_REFS", "PURGE_QUESTION_EVIDENCE", "REDACT_QUESTIONS", "PURGE_QUESTION_STEPS",
		"REDACT_CONVERSATIONS", "REDACT_UPLOADS", "REDACT_PARSE_TASKS", "PURGE_CONTENT_HEADS",
		"PURGE_CONTENT_SOURCE_REFS", "PURGE_CONTENT_ARTIFACTS", "PURGE_CONTENT_JOB_BATCHES", "PURGE_CONTENT_JOBS",
		"PURGE_TASK_STEPS", "PURGE_EMBEDDING_BATCHES", "PURGE_ASR_SUBMISSIONS", "PURGE_KAFKA_REJECTIONS",
		"PURGE_INBOX_EVENTS", "PURGE_OUTBOX_EVENTS", "PURGE_PARSE_RESULTS", "PURGE_CHUNK_EMBEDDINGS",
		"PURGE_EMBEDDING_SETS", "PURGE_CHUNK_SOURCES", "PURGE_CHUNKS", "PURGE_CHUNK_SETS",
		"PURGE_TRANSCRIPT_SEGMENTS", "PURGE_TRANSCRIPTS", "PURGE_BLOBS", "PURGE_GC_OBJECTS",
		"PURGE_GC_OBJECTS_FINAL", "FINALIZE", "content_job",
	}, "other")
	outcome = boundedLabel(outcome, []string{"success", "failure", "retry_wait", "lease_lost", "shutdown"}, "other")
	labels := []string{m.service, worker, stage, outcome}
	m.workerRuns.WithLabelValues(labels...).Inc()
	m.workerDuration.WithLabelValues(labels...).Observe(duration.Seconds())
}

func (m *Metrics) ObserveWorkerActive(worker string, delta float64) {
	if m == nil {
		return
	}
	worker = boundedLabel(worker, []string{"media", "content", "deletion"}, "other")
	m.activeWorkers.WithLabelValues(m.service, worker).Add(delta)
}

func NewHTTPServer(addr string, metrics *Metrics) (*http.Server, net.Listener, error) {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.Handler())
	return &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    1 << 20,
	}, listener, nil
}

func boundedLabel(value string, allowed []string, fallback string) string {
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	return fallback
}
