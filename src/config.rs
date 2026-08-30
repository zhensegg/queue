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

    #[arg(long)]
    pub auth_token: Option<String>,

    #[arg(long)]
    pub auth_token_file: Option<String>,

    #[arg(long)]
    pub tls_cert: Option<String>,

    #[arg(long)]
    pub tls_key: Option<String>,

    #[arg(long)]
    pub http_auth_token: Option<String>,

    #[arg(long)]
    pub http_auth_token_file: Option<String>,

    #[arg(long)]
    pub http_loopback_only: bool,

    #[arg(long)]
    pub max_connections: Option<usize>,

    #[arg(long, default_value_t = 10)]
    pub auth_timeout_secs: u64,

    #[arg(long, default_value_t = true)]
    pub durable_acks: bool,

    #[arg(long, default_value_t = 10)]
    pub durable_ack_timeout_secs: u64,

    #[arg(long, default_value = "overwrite")]
    pub on_overflow: String,
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
