package observability

import (
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
)

type pgxPoolCollector struct {
	service string
	pool    *pgxpool.Pool
	descs   map[string]*prometheus.Desc
}

func newPGXPoolCollector(service string, pool *pgxpool.Pool) *pgxPoolCollector {
	const help = "PostgreSQL connection pool statistics exported by pgxpool."
	descs := make(map[string]*prometheus.Desc)
	for _, name := range []string{
		"mirrorcore_db_pool_max_connections",
		"mirrorcore_db_pool_total_connections",
		"mirrorcore_db_pool_acquired_connections",
		"mirrorcore_db_pool_idle_connections",
		"mirrorcore_db_pool_constructing_connections",
		"mirrorcore_db_pool_acquires_total",
		"mirrorcore_db_pool_acquire_duration_seconds_total",
		"mirrorcore_db_pool_canceled_acquires_total",
		"mirrorcore_db_pool_empty_acquires_total",
		"mirrorcore_db_pool_empty_acquire_wait_seconds_total",
		"mirrorcore_db_pool_new_connections_total",
		"mirrorcore_db_pool_max_idle_destroyed_connections_total",
		"mirrorcore_db_pool_max_lifetime_destroyed_connections_total",
	} {
		descs[name] = prometheus.NewDesc(name, help, []string{"service"}, nil)
	}
	return &pgxPoolCollector{service: service, pool: pool, descs: descs}
}

func (c *pgxPoolCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs {
		ch <- desc
	}
}

func (c *pgxPoolCollector) Collect(ch chan<- prometheus.Metric) {
	stat := c.pool.Stat()
	values := map[string]struct {
		value float64
		type_ prometheus.ValueType
	}{
		"mirrorcore_db_pool_max_connections":                          {float64(stat.MaxConns()), prometheus.GaugeValue},
		"mirrorcore_db_pool_total_connections":                        {float64(stat.TotalConns()), prometheus.GaugeValue},
		"mirrorcore_db_pool_acquired_connections":                     {float64(stat.AcquiredConns()), prometheus.GaugeValue},
		"mirrorcore_db_pool_idle_connections":                         {float64(stat.IdleConns()), prometheus.GaugeValue},
		"mirrorcore_db_pool_constructing_connections":                 {float64(stat.ConstructingConns()), prometheus.GaugeValue},
		"mirrorcore_db_pool_acquires_total":                           {float64(stat.AcquireCount()), prometheus.CounterValue},
		"mirrorcore_db_pool_acquire_duration_seconds_total":           {stat.AcquireDuration().Seconds(), prometheus.CounterValue},
		"mirrorcore_db_pool_canceled_acquires_total":                  {float64(stat.CanceledAcquireCount()), prometheus.CounterValue},
		"mirrorcore_db_pool_empty_acquires_total":                     {float64(stat.EmptyAcquireCount()), prometheus.CounterValue},
		"mirrorcore_db_pool_empty_acquire_wait_seconds_total":         {stat.EmptyAcquireWaitTime().Seconds(), prometheus.CounterValue},
		"mirrorcore_db_pool_new_connections_total":                    {float64(stat.NewConnsCount()), prometheus.CounterValue},
		"mirrorcore_db_pool_max_idle_destroyed_connections_total":     {float64(stat.MaxIdleDestroyCount()), prometheus.CounterValue},
		"mirrorcore_db_pool_max_lifetime_destroyed_connections_total": {float64(stat.MaxLifetimeDestroyCount()), prometheus.CounterValue},
	}
	for name, item := range values {
		ch <- prometheus.MustNewConstMetric(c.descs[name], item.type_, item.value, c.service)
	}
}
