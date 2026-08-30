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

    /// Optional PEM certificate chain for TLS. When set together with
    /// --tls-key the data plane serves TLS on `addr`.
    #[arg(long)]
    pub tls_cert: Option<String>,

    /// Optional PEM private key for TLS.
    #[arg(long)]
    pub tls_key: Option<String>,
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
