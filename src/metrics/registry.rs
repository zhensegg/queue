//! Prometheus metrics registry.

use std::time::Instant;

use prometheus::{
    Counter, CounterVec, Encoder, Gauge, HistogramOpts, HistogramVec, Opts, Registry, TextEncoder,
};

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,
    pub messages_total: CounterVec,
    pub messages_bytes_total: CounterVec,
    pub connections_total: Gauge,
    pub subscriptions_total: Gauge,
    pub backlog_size: Gauge,
    pub store_usage_bytes: Gauge,
    pub append_latency: HistogramVec,
    pub fsync_latency: HistogramVec,
    pub broker_uptime: Gauge,
    pub auth_failures_total: Counter,
    pub auth_successes_total: Counter,
    start_time: Instant,
}

impl Metrics {
    pub fn new() -> Self {
        let registry = Registry::new();

        let messages_total = CounterVec::new(
            Opts::new("zhensegg_messages_total", "Total messages"),
            &["type"],
        ).unwrap();

        let messages_bytes_total = CounterVec::new(
            Opts::new("zhensegg_messages_bytes_total", "Total message bytes"),
            &["type"],
        ).unwrap();

        let connections_total = Gauge::with_opts(
            Opts::new("zhensegg_connections_total", "Active connections"),
        ).unwrap();

        let subscriptions_total = Gauge::with_opts(
            Opts::new("zhensegg_subscriptions_total", "Active subscriptions"),
        ).unwrap();

        let backlog_size = Gauge::with_opts(
            Opts::new("zhensegg_backlog_size", "Undelivered messages"),
        ).unwrap();

        let store_usage_bytes = Gauge::with_opts(
            Opts::new("zhensegg_store_usage_bytes", "Store usage in bytes"),
        ).unwrap();

        let buckets = vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0];
        let append_latency = HistogramVec::new(
            HistogramOpts::new("zhensegg_append_latency_seconds", "Append latency")
                .buckets(buckets.clone()),
            &["type"],
        ).unwrap();

        let fsync_latency = HistogramVec::new(
            HistogramOpts::new("zhensegg_fsync_latency_seconds", "Fsync latency")
                .buckets(buckets),
            &["type"],
        ).unwrap();

        let broker_uptime = Gauge::with_opts(
            Opts::new("zhensegg_broker_uptime_seconds", "Broker uptime"),
        ).unwrap();

        let auth_failures_total = Counter::with_opts(
            Opts::new("zhensegg_auth_failures_total", "Rejected connections (auth failures)"),
        ).unwrap();

        let auth_successes_total = Counter::with_opts(
            Opts::new("zhensegg_auth_successes_total", "Authenticated connections"),
        ).unwrap();

        registry.register(Box::new(messages_total.clone())).unwrap();
        registry.register(Box::new(messages_bytes_total.clone())).unwrap();
        registry.register(Box::new(connections_total.clone())).unwrap();
        registry.register(Box::new(subscriptions_total.clone())).unwrap();
        registry.register(Box::new(backlog_size.clone())).unwrap();
        registry.register(Box::new(store_usage_bytes.clone())).unwrap();
        registry.register(Box::new(append_latency.clone())).unwrap();
        registry.register(Box::new(fsync_latency.clone())).unwrap();
        registry.register(Box::new(broker_uptime.clone())).unwrap();
        registry.register(Box::new(auth_failures_total.clone())).unwrap();
        registry.register(Box::new(auth_successes_total.clone())).unwrap();

        Self {
            registry,
            messages_total,
            messages_bytes_total,
            connections_total,
            subscriptions_total,
            backlog_size,
            store_usage_bytes,
            append_latency,
            fsync_latency,
            broker_uptime,
            auth_failures_total,
            auth_successes_total,
            start_time: Instant::now(),
        }
    }

    pub fn uptime_seconds(&self) -> f64 {
        self.start_time.elapsed().as_secs_f64()
    }

    pub fn render(&self) -> String {
        self.broker_uptime.set(self.uptime_seconds());
        let encoder = TextEncoder::new();
        let metric_families = self.registry.gather();
        let mut buffer = Vec::new();
        encoder.encode(&metric_families, &mut buffer).unwrap();
        String::from_utf8(buffer).unwrap_or_default()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::new()
    }
}
