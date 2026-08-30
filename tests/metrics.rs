use zhensegg::Metrics;

#[test]
fn metrics_new_creates_all_metric_families() {
    let m = Metrics::new();
    m.messages_total.with_label_values(&["published"]).inc();
    m.connections_total.inc();
    m.connections_total.inc();
    m.subscriptions_total.inc();
    m.backlog_size.set(7.0);
    m.store_usage_bytes.set(1000.0);
}

#[test]
fn metrics_render_exposes_prometheus_names() {
    let m = Metrics::new();
    m.messages_total.with_label_values(&["published"]).inc();
    m.messages_bytes_total.with_label_values(&["published"]).inc_by(123.0);
    m.connections_total.inc();
    m.subscriptions_total.inc();
    m.backlog_size.set(5.0);
    m.store_usage_bytes.set(999.0);
    m.append_latency.with_label_values(&["disk"]).observe(0.001);
    m.fsync_latency.with_label_values(&["disk"]).observe(0.002);

    let out = m.render();
    for name in [
        "zhensegg_messages_total",
        "zhensegg_messages_bytes_total",
        "zhensegg_connections_total",
        "zhensegg_subscriptions_total",
        "zhensegg_backlog_size",
        "zhensegg_store_usage_bytes",
        "zhensegg_append_latency_seconds",
        "zhensegg_fsync_latency_seconds",
        "zhensegg_broker_uptime_seconds",
    ] {
        assert!(out.contains(name), "expected metric {name} in render output");
    }
}

#[test]
fn metrics_render_reflects_counter_values() {
    let m = Metrics::new();
    m.messages_total.with_label_values(&["published"]).inc_by(3.0);
    let out = m.render();
    assert!(
        out.contains("zhensegg_messages_total{type=\"published\"} 3"),
        "expected counter value, got:\n{out}"
    );
}

#[test]
fn metrics_ready_and_up() {
    let m = Metrics::new();
    // uptime immediately after construction should be a small positive number
    assert!(m.uptime_seconds() >= 0.0);
}

#[test]
fn metrics_uptime_increases() {
    let m = Metrics::new();
    let t0 = m.uptime_seconds();
    std::thread::sleep(std::time::Duration::from_millis(50));
    let t1 = m.uptime_seconds();
    assert!(t1 > t0);
}

#[test]
fn metrics_is_clonable_shared_registry() {
    let m = Metrics::new();
    let m2 = m.clone();
    m2.connections_total.inc();
    assert_eq!(m.connections_total.get() as u64, 1, "clone shares the gauge");
}
