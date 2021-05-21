#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

use anyhow::{bail, Result};
use biliapi::Request;
use clap::Clap;
use futures::StreamExt;
use influxdb_client::{Client as InfluxClient, Precision};
use std::time::{Duration, Instant};

mod messages;
pub use messages::Message;

mod process;

#[derive(Debug, clap::Clap)]
struct Opts {
    #[clap(about = "The live room id")]
    room_ids: Vec<u64>,

    #[clap(
        long = "influx",
        short = 'i',
        about = "The influx db url",
        default_value = "http://localhost:8086"
    )]
    influx_db_url: String,

    #[clap(
        long = "db",
        short = 'd',
        about = "The influx db",
        default_value = "ddpanel"
    )]
    influx_db: String,

    #[clap(
        long = "token",
        short = 't',
        about = "The influx token. If missing will try to retrieve from env INFLUX_TOKEN"
    )]
    influx_token: Option<String>,

    #[clap(long = "org", short = 'o')]
    influx_org: String,

    #[clap(long = "bucket", short = 'b')]
    influx_bucket: String,
}

async fn start_live_monitor(
    long_room_id: u64,
    client: &reqwest::Client,
    influx: &InfluxClient,
) -> Result<()> {
    // 拿到弹幕数据
    let danmu_info = biliapi::requests::DanmuInfo::request(&client, long_room_id).await?;
    let server = &danmu_info.servers[0];
    let url = server.url();

    let mut connection =
        biliapi::connection::LiveConnection::new(&url, long_room_id, danmu_info.token).await?;
    info!("room {} connected.", long_room_id);
    while let Some(msg) = connection.next().await {
        match msg {
            Ok(msg) => process::on_packet(msg, long_room_id, influx).await?,
            Err(e) => {
                error!("error: {:?}", e);
                return Err(e.into());
            }
        }
    }
    anyhow::bail!("Connection ran out.")
}

async fn start_live_monitor_with_retry(
    room_id: u64,
    client: reqwest::Client,
    influx: InfluxClient,
) -> Result<()> {
    info!("run_with_retry: room_id = {}", room_id);
    let room_info = biliapi::requests::InfoByRoom::request(&client, room_id).await?;
    let long_room_id = room_info.room_info.room_id;

    let mut last_time = Instant::now();
    let mut err_counter = 0;
    static ALLOW_FAIL_DURATION: Duration = Duration::from_secs(5 * 60);
    loop {
        match start_live_monitor(long_room_id, &client, &influx).await {
            Ok(_) => unreachable!(),
            Err(e) => {
                warn!("发生错误：{:?}", e);
                if Instant::now().duration_since(last_time) > ALLOW_FAIL_DURATION {
                    err_counter += 1;
                    if err_counter > 5 {
                        return Err(e);
                    }
                } else {
                    info!(
                        "距离上次失败已经过去了 {:?}",
                        Instant::now().duration_since(last_time)
                    );
                    err_counter = 1;
                }
                last_time = Instant::now();
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init();

    let opts = Opts::parse();
    debug!("opts = {:?}", opts);

    let token = match opts.influx_token {
        Some(t) => t,
        None => match std::env::var("INFLUX_TOKEN") {
            Ok(s) => s,
            Err(_) => bail!("Set env INFLUX_TOKEN or pass it in the influx_token arg."),
        },
    };

    let client = biliapi::connection::new_client()?;

    let influx = InfluxClient::new(opts.influx_db_url, token)?
        .with_org(opts.influx_org)
        .with_bucket(opts.influx_bucket)
        .with_precision(Precision::MS);

    let mut handlers = vec![];
    for room_id in opts.room_ids {
        let handler = tokio::spawn(start_live_monitor_with_retry(
            room_id,
            client.clone(),
            influx.clone(),
        ));
        handlers.push(handler);
    }

    let future = futures::future::join_all(handlers);
    let _results = future.await;
    Ok(())
}
