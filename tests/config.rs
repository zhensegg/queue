use clap::Parser;
use zhensegg::Config;

#[test]
fn config_defaults() {
    let c = Config::parse_from(["zhensegg-broker"]);
    assert_eq!(c.addr, "0.0.0.0:9090");
    assert_eq!(c.http_addr, "0.0.0.0:9091");
    assert_eq!(c.cores, 1);
    assert_eq!(c.mode, "mem");
    assert_eq!(c.mem_mb, 256);
    assert_eq!(c.file, "/tmp/zhensegg.ring");
    assert_eq!(c.ring_capacity_mb, 1_000_000);
}

#[test]
fn config_parses_explicit_values() {
    let c = Config::parse_from([
        "zhensegg-broker",
        "--addr", "127.0.0.1:7000",
        "--http-addr", "127.0.0.1:7001",
        "--cores", "4",
        "--mode", "file",
        "--mem-mb", "1024",
        "--file", "/var/lib/zhensegg/ring.dat",
        "--ring-capacity-mb", "2048",
    ]);
    assert_eq!(c.addr, "127.0.0.1:7000");
    assert_eq!(c.http_addr, "127.0.0.1:7001");
    assert_eq!(c.cores, 4);
    assert_eq!(c.mode, "file");
    assert_eq!(c.mem_mb, 1024);
    assert_eq!(c.file, "/var/lib/zhensegg/ring.dat");
    assert_eq!(c.ring_capacity_mb, 2048);
}

#[test]
fn config_mem_capacity_bytes() {
    let mut c = Config::parse_from(["zhensegg-broker"]);
    c.mem_mb = 2;
    assert_eq!(c.mem_capacity_bytes(), 2 * 1024 * 1024);
}

#[test]
fn config_ring_capacity_bytes() {
    let mut c = Config::parse_from(["zhensegg-broker"]);
    c.ring_capacity_mb = 3;
    assert_eq!(c.ring_capacity_bytes(), 3 * 1024 * 1024);
}

#[test]
fn config_mode_detection() {
    let mem = Config::parse_from(["zhensegg-broker"]);
    assert!(!mem.is_file_mode());

    let file = Config::parse_from(["zhensegg-broker", "--mode", "file"]);
    assert!(file.is_file_mode());
}
