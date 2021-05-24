use std::time::Duration;

use anyhow::{Context, Result};
use biliapi::ws_protocol::{KnownOperation, Operation, Packet};
use chrono::{DateTime, Local};
use influxdb_client::Client as InfluxClient;
use tokio::{
    sync::broadcast::{self, error::RecvError},
    time,
};

use super::{messages::*, CachedInfluxClient, RoomInfo};

const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

/// 解析接收到的 packet，向 influxdb 插入
///
/// 提供一个 async_writer 选项，在此模式下会将写入放到后台执行，默认开启
pub struct InfluxAppender {
    client: CachedInfluxClient,
    /// 接收 packet
    packets_receiver: broadcast::Receiver<Packet>,
}

impl InfluxAppender {
    pub fn new(client: InfluxClient, packets_receiver: broadcast::Receiver<Packet>) -> Self {
        let client = CachedInfluxClient::new(client);
        Self {
            client,
            packets_receiver,
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.client = self.client.buffer_size(buffer_size);
        self
    }

    #[allow(unused)]
    pub fn async_write(mut self, async_write: bool) -> Self {
        self.client = self.client.async_write(async_write);
        self
    }

    pub async fn start(mut self) -> Result<()> {
        self.start_writer().await;
        info!("the channel is closed. flushing remaining caches");
        self.client.flush().await?;
        Ok(())
    }

    /// 处理循环
    async fn start_writer(&mut self) {
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=6ba08f53a430171c7791ac5d0dd15a84
        let mut flush_interval = time::interval(FLUSH_INTERVAL);
        loop {
            // 最多等 2s，超时会检查尝试 flush
            tokio::select! {
                _ = flush_interval.tick() => {
                    if let Err(e) = self.client.flush().await {
                        warn!("flush failed: {:?}", e);
                    }
                },
                recv = self.packets_receiver.recv() => {
                    match recv {
                        Ok(packet) => match self.process_packet(packet).await {
                            Ok(_) => {}
                            Err(e) => {
                                // NOTE: 允许 packet 处理失败
                                warn!("Failed to process packet: {:?}", e);
                            }
                        },
                        Err(RecvError::Lagged(cnt)) => {
                            // NOTE: 允许丢包，主程序继续运行
                            error!("Influxdb write too slow and lagged {} packets!", cnt);
                            continue;
                        }
                        Err(RecvError::Closed) => {
                            info!("packet publisher closed. influx appender stop.");
                            return;
                        }
                    }
                }
            }
        }
    }

    async fn process_packet(&mut self, packet: Packet) -> Result<()> {
        let t = packet.time;
        let room_id = packet.room_id;

        match packet.operation {
            Operation::Known(KnownOperation::SendMsgReply) => {
                let room_info = RoomInfo::from_cache(room_id);

                let msg = serde_json::from_str::<SendMsgReply>(&packet.body)
                    .context("转换 SendMsgReply 失败")?;

                debug!("msg cmd: {}", msg.cmd);
                self.on_send_msg_reply(msg, &room_info, t).await
            }
            Operation::Known(KnownOperation::HeartbeatReply) => {
                let room_info = RoomInfo::from_cache(room_id);

                let popularity: i64 = packet.body.parse()?;
                self.on_popularity(popularity, &room_info, t).await
            }
            _ => Ok(()),
        }
    }

    async fn on_send_msg_reply(
        &mut self,
        msg: SendMsgReply,
        room_info: &RoomInfo,
        t: DateTime<Local>,
    ) -> Result<()> {
        let point = match msg.cmd.as_str() {
            "SUPER_CHAT_MESSAGE" 
            // | "SUPER_CHAT_MESSAGE_JPN" 日语翻译的会推送两遍导致重复计费
            => {
                let sc: SuperChat =
                    serde_json::from_value(msg.data).context("convert msg to super chat failed")?;
                info!("SC: {} @ {}", sc, room_info.streamer);
                sc.into_point(room_info, t)
            }
            "SEND_GIFT" => {
                let gift: SendGift =
                    serde_json::from_value(msg.data).context("convert msg to send gift failed")?;
                // 不统计免费礼物
                if gift.is_free() {
                    return Ok(());
                }
                info!("礼物: {} @ {}", gift, room_info.streamer);
                gift.into_point(room_info, t)
            }
            "USER_TOAST_MSG" => {
                let guard: UserToastMsg = serde_json::from_value(msg.data)
                    .context("convert msg to UserToastMsg failed.")?;
                info!("舰长: {} @ {}", guard, room_info.streamer);
                guard.into_point(room_info, t)
            }
            "DANMU_MSG" => {
                // TODO: 统计弹幕
                return Ok(());
            }
            _ => return Ok(()),
        };
        self.client.insert_point(point).await?;
        Ok(())
    }

    async fn on_popularity(
        &mut self,
        popularity: i64,
        room_info: &RoomInfo,
        t: DateTime<Local>,
    ) -> Result<()> {
        let p = Popularity::new(popularity);
        if p.value > 1 {
            debug!("room popularity: {:?}", p);
        }
        let point = p.into_point(room_info, t);
        self.client.insert_point(point).await?;
        Ok(())
    }
}
