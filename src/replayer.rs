use anyhow::{Context, Result};
use biliapi::ws_protocol::Packet;
use reqwest::Client as HttpClient;
use std::time::Duration;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, BufReader},
    sync::broadcast,
};

pub struct FileReplayer {
    broadcaster: broadcast::Sender<Packet>,
    http_client: HttpClient,
    replay_delay: Duration,
}

impl FileReplayer {
    pub async fn new(
        broadcaster: broadcast::Sender<Packet>,
        http_client: HttpClient,
        replay_delay_ms: u32,
    ) -> Result<Self> {
        Ok(Self {
            broadcaster,
            http_client,
            replay_delay: Duration::from_millis(replay_delay_ms as u64),
        })
    }

    pub async fn replay(&mut self, path: String) -> Result<()> {
        info!("replaying file {:?}", path);
        let f = File::open(&path).await?;
        let f = BufReader::new(f);
        if path.ends_with("gz") {
            info!("gz detected, treating as gz");
            let reader = async_compression::tokio::bufread::GzipDecoder::new(f);
            let reader = BufReader::new(reader);
            self.run(reader).await
        } else {
            self.run(f).await
        }
    }

    pub async fn run<R>(&mut self, reader: R) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
    {
        let mut cnt = 0;
        let mut lines = reader.lines();
        loop {
            let line = lines.next_line().await.context("Failed to parse line")?;
            let line = match line {
                Some(l) => l,
                None => {
                    info!("replay file finished.");
                    break Ok(());
                }
            };
            let packet: Packet = serde_json::from_str(&line)?;

            use crate::influx::RoomInfo;
            use biliapi::Request;

            if RoomInfo::from_cache_opt(packet.room_id).is_none() {
                let info =
                    biliapi::requests::InfoByRoom::request(&self.http_client, packet.room_id)
                        .await?;
                RoomInfo::write_cache(packet.room_id, info.anchor_info.base.uname);
            }

            cnt += 1;
            if cnt % 1_000 == 0 {
                info!("{} packets replayed, t = {}", cnt, packet.time);
                if self.replay_delay.as_micros() > 0 {
                    tokio::time::sleep(self.replay_delay).await;
                }
            }

            self.broadcaster.send(packet)?;
        }
    }
}
