use anyhow::Result;
use biliapi::ws_protocol::Packet;
use reqwest::Client as HttpClient;
use std::path::PathBuf;
use tokio::{
    fs::File,
    io::{AsyncBufReadExt, BufReader},
    sync::broadcast,
};

pub struct FileReplayer {
    reader: BufReader<File>,
    broadcaster: broadcast::Sender<Packet>,
    http_client: HttpClient,
}

impl FileReplayer {
    pub async fn new(
        path: PathBuf,
        broadcaster: broadcast::Sender<Packet>,
        http_client: HttpClient,
    ) -> Result<Self> {
        let f = File::open(&path).await?;
        let reader = BufReader::new(f);
        Ok(Self {
            reader,
            broadcaster,
            http_client,
        })
    }

    pub async fn start(self) -> Result<()> {
        let mut cnt = 0;
        let mut lines = self.reader.lines();
        while let Some(line) = lines.next_line().await? {
            let packet: Packet = serde_json::from_str(&line)?;

            use crate::influx::RoomInfo;
            use biliapi::Request;

            if RoomInfo::from_cache_opt(packet.room_id).is_none() {
                let info =
                    biliapi::requests::InfoByRoom::request(&self.http_client, packet.room_id)
                        .await?;
                RoomInfo::write_cache(packet.room_id, info.anchor_info.base.uname);
            }

            self.broadcaster.send(packet)?;

            cnt += 1;
            if cnt % 1_000 == 0 {
                info!("{} packets replayed.", cnt);
            }
        }
        Ok(())
    }
}
