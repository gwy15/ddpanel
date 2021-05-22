#[macro_use]
extern crate serde;
#[macro_use]
extern crate log;

use anyhow::Result;
use influxdb_client::{Client as InfluxClient, Precision};

mod file_appender;
mod influx;
mod manager;
mod monitor;
mod task_factory;

use manager::Manager;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv::dotenv()?;
    log4rs::init_file("log4rs.yml", Default::default())?;

    let influx_client = InfluxClient::new("http://127.0.0.1:8086", std::env::var("INFLUX_TOKEN")?)?
        .with_org("ddpanel")
        .with_bucket("ddpanel")
        .with_precision(Precision::MS);

    Manager::new()
        .influx_appender(influx_client)
        .file_appender("recorded.json".into())
        .await?
        .start("rooms.json".into())
        .await?;
    Ok(())
}
