#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

use anyhow::Result;
use clap::Clap;
use influxdb_client::{Client as InfluxClient, Precision};
use std::path::PathBuf;

mod file_appender;
mod influx;
mod manager;
mod monitor;
mod replayer;
mod task_factory;

use manager::Manager;

#[derive(Debug, clap::Clap)]
struct Opts {
    #[clap(
        long = "record-output",
        short = 'o',
        default_value = "recorded-%.json.gz"
    )]
    record_file: String,

    #[clap(
        long = "no-file",
        about = "Do not output to file. For replay mode it is always enabled."
    )]
    no_file: bool,

    #[clap(long = "no-influx", about = "Do not write to influxdb.")]
    no_influx: bool,

    #[clap(long = "replay", short = 'r', about = "Replay the file")]
    replay: Option<String>,

    #[clap(
        long = "replay-delay",
        short = 's',
        default_value = "100",
        about = "Replay the file with a slight delay every 1000 packets."
    )]
    replay_delay_ms: u32,

    #[clap(
        long = "watch",
        short = 'w',
        default_value = "watch_rooms",
        about = "The room file to watch"
    )]
    watch: PathBuf,
}

impl Opts {
    pub fn influx_client(&self) -> Result<InfluxClient> {
        Ok(
            InfluxClient::new("http://127.0.0.1:8086", std::env::var("INFLUX_TOKEN")?)?
                .with_org("ddpanel")
                .with_bucket("ddpanel")
                .with_precision(Precision::MS),
        )
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv()?;
    log4rs::init_file("log4rs.yml", Default::default())?;

    let mut opts = Opts::parse();

    let mut manager = Manager::new();

    if opts.replay.is_some() {
        opts.no_file = true;
    }

    if !opts.no_influx {
        let buffer_size = if opts.replay.is_some() { 128 } else { 0 };
        manager = manager.influx_appender(opts.influx_client()?, buffer_size);
    }
    if !opts.no_file {
        manager = manager.file_appender(opts.record_file).await?;
    }
    if opts.no_file && opts.no_influx {
        // 至少要一个 appender 才可以
        manager = manager.no_appender();
    }

    if let Some(replay) = opts.replay {
        // always disable file output
        manager.replay(replay, opts.replay_delay_ms).await?;
    } else {
        // start record
        manager.start(opts.watch).await?;
    }

    Ok(())
}
