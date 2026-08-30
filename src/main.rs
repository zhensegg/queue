use clap::Parser;

use zhensegg::broker;
use zhensegg::config::Config;

fn main() -> anyhow::Result<()> {
    let config = Config::parse();
    broker::run_broker(config)
}
