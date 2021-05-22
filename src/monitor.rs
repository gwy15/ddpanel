use anyhow::{anyhow, bail, Result};
use biliapi::{ws_protocol::Packet, Request};
use futures::StreamExt;
use reqwest::Client as HttpClient;
use std::time::{Duration, Instant};
use tokio::sync::{broadcast, oneshot};

static ALLOW_FAIL_DURATION: Duration = Duration::from_secs(5 * 60);

pub struct Monitor {
    room_id: u64,
    broadcaster: broadcast::Sender<Packet>,
    http_client: HttpClient,
}

impl Monitor {
    pub fn new(
        room_id: u64,
        broadcaster: broadcast::Sender<Packet>,
        http_client: HttpClient,
    ) -> Self {
        Self {
            room_id,
            broadcaster,
            http_client,
        }
    }
    pub async fn start(mut self, terminate_receiver: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = terminate_receiver => {
                info!("Room monitor {} received stop signal. Stopping.", self.room_id);
                Ok(())
            }
            ret = self.start_live_monitor_with_retry() => {
                ret
            }
        }
    }

    async fn start_live_monitor_with_retry(&mut self) -> Result<()> {
        info!("run_with_retry: room_id = {}", self.room_id);
        let room_info =
            biliapi::requests::InfoByRoom::request(&self.http_client, self.room_id).await?;
        let long_room_id = room_info.room_info.room_id;

        let mut last_time = Instant::now();
        let mut err_counter = 0;

        loop {
            match self.live_monitor(long_room_id).await {
                Ok(_) => unreachable!(),
                Err(e) => {
                    warn!("发生错误：{:?}", e);
                    if Instant::now().duration_since(last_time) < ALLOW_FAIL_DURATION {
                        warn!("错误发生过于频繁！");
                        err_counter += 1;
                        if err_counter > 5 {
                            error!("错误发生过于频繁！取消任务！");
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

    async fn live_monitor(&mut self, long_room_id: u64) -> Result<()> {
        // 拿到弹幕数据
        let danmu_info =
            biliapi::requests::DanmuInfo::request(&self.http_client, long_room_id).await?;
        let server = &danmu_info.servers[0];
        let url = server.url();

        let mut connection =
            biliapi::connection::LiveConnection::new(&url, long_room_id, danmu_info.token).await?;
        info!("room {} connected.", long_room_id);
        while let Some(packet) = connection.next().await {
            match packet {
                Ok(packet) => {
                    self.broadcaster
                        .send(packet)
                        .map_err(|_| anyhow!("Cannot send packet!"))?;
                }
                Err(e) => {
                    error!("error: {:?}", e);
                    return Err(e.into());
                }
            }
        }
        bail!("Connection ran out.")
    }
}
