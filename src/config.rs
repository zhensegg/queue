use clap::Parser;

#[derive(Parser, Debug, Clone)]
#[command(name = "zhensegg-broker")]
pub struct Config {
    #[arg(long, default_value = "0.0.0.0:9090")]
    pub addr: String,

    #[arg(long, default_value = "0.0.0.0:9091")]
    pub http_addr: String,

    #[arg(long, default_value = "1")]
    pub cores: usize,

    #[arg(long, default_value = "mem")]
    pub mode: String,

    #[arg(long, default_value = "256")]
    pub mem_mb: usize,

    #[arg(long, default_value = "/tmp/zhensegg.ring")]
    pub file: String,

    #[arg(long, default_value = "1000000")]
    pub ring_capacity_mb: usize,

    /// Optional shared token. When set, every client must present it as its
    /// first Auth frame before any other command is accepted.
    #[arg(long)]
    pub auth_token: Option<String>,

    /// Optional path to a file holding the shared token. If set (and takes
    /// precedence over --auth-token), the token is re-read from this file on
    /// every SIGHUP so it can be rotated without restarting the broker.
    #[arg(long)]
    pub auth_token_file: Option<String>,

    /// Optional PEM certificate chain for TLS. When set together with
    /// --tls-key the data plane serves TLS on `addr`.
    #[arg(long)]
    pub tls_cert: Option<String>,

    /// Optional PEM private key for TLS.
    #[arg(long)]
    pub tls_key: Option<String>,

    /// Optional token protecting the HTTP admin plane (/metrics, /health,
    /// /ready). When set, these endpoints require HTTP Basic auth.
    #[arg(long)]
    pub http_auth_token: Option<String>,

    /// Optional path to a file holding the HTTP admin token. If set (takes
    /// precedence over --http-auth-token), it is re-read on every SIGHUP.
    #[arg(long)]
    pub http_auth_token_file: Option<String>,

    /// Bind the HTTP admin server to the loopback interface only, regardless
    /// of --http-addr. Safer default when you only need local /metrics.
    #[arg(long)]
    pub http_loopback_only: bool,

    /// Maximum number of concurrent data-plane connections accepted per
    /// accept loop (default: unlimited). Prevents fd/memory exhaustion.
    #[arg(long)]
    pub max_connections: Option<usize>,

    /// How long a connection has to complete authentication (or a TLS
    /// handshake) before it is dropped, in seconds (default: 10).
    #[arg(long, default_value_t = 10)]
    pub auth_timeout_secs: u64,
}

impl Config {
    pub fn ring_capacity_bytes(&self) -> usize {
        self.ring_capacity_mb * 1024 * 1024
    }

    pub fn mem_capacity_bytes(&self) -> usize {
        self.mem_mb * 1024 * 1024
    }

    pub fn is_file_mode(&self) -> bool {
        self.mode == "file"
    }
}
