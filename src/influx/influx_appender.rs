use anyhow::{Context, Result};
use biliapi::ws_protocol::{KnownOperation, Operation, Packet};
use chrono::{DateTime, Local};
use influxdb_client::{Client as InfluxClient, Point, TimestampOptions};
use tokio::sync::{broadcast, oneshot};

use super::{messages::*, RoomInfo};

pub struct InfluxAppender {
    insert_count: u64,
    buffer: Vec<Point>,
    buffer_size: usize,
    client: InfluxClient,
    packets_receiver: broadcast::Receiver<Packet>,
}

impl InfluxAppender {
    pub fn new(client: InfluxClient, packets_receiver: broadcast::Receiver<Packet>) -> Self {
        Self {
            insert_count: 0,
            client,
            buffer: vec![],
            buffer_size: 0,
            packets_receiver,
        }
    }

    pub fn buffer(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    pub async fn start(mut self, terminate_receiver: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = terminate_receiver => {
                info!("terminate.");
                self.flush().await?;
                Ok(())
            },
            r = self.start_writer() => {
                self.flush().await?;
                r
            }
        }
    }

    async fn start_writer(&mut self) -> Result<()> {
        loop {
            let recv = self.packets_receiver.recv().await;
            match recv {
                Ok(packet) => match self.process_packet(packet).await {
                    Ok(_) => {}
                    Err(e) => {
                        warn!("Failed to process packet: {:?}", e);
                    }
                },
                Err(e) => {
                    error!("recv error: {:?}", e);
                    return Err(e.into());
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
            "SUPER_CHAT_MESSAGE" | "SUPER_CHAT_MESSAGE_JPN" => {
                let sc: SuperChat =
                    serde_json::from_value(msg.data).context("convert msg to super chat failed")?;
                info!("sc: {:?}", sc);
                sc.into_point(room_info, t)
            }
            "SEND_GIFT" => {
                let gift: SendGift =
                    serde_json::from_value(msg.data).context("convert msg to send gift failed")?;
                // 不统计免费礼物
                if gift.is_free() {
                    return Ok(());
                }
                info!("gift: {:?}", gift);
                gift.into_point(room_info, t)
            }
            "USER_TOAST_MSG" => {
                let guard: UserToastMsg = serde_json::from_value(msg.data)
                    .context("convert msg to UserToastMsg failed.")?;
                info!("guard buy: {:?}", guard);
                guard.into_point(room_info, t)
            }
            "DANMU_MSG" => {
                // TODO: 统计弹幕
                return Ok(());
            }
            _ => return Ok(()),
        };
        self.insert(point).await?;
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
        self.insert(point).await?;
        Ok(())
    }

    async fn insert(&mut self, point: Point) -> Result<()> {
        self.insert_count += 1;

        if self.buffer_size == 0 {
            self.client
                .insert_points(&[point], TimestampOptions::FromPoint)
                .await?;
        } else {
            self.buffer.push(point);
            if self.buffer.len() == self.buffer_size {
                self.flush().await?;
            }
        }

        if self.insert_count % 100 == 0 {
            info!("{} points inserted to influxdb.", self.insert_count);
        }
        Ok(())
    }

    /// 一定会写入
    async fn flush(&mut self) -> Result<()> {
        use std::time::Instant;
        if self.buffer_size > 0 {
            let t = Instant::now();

            self.client
                .insert_points(&self.buffer, TimestampOptions::FromPoint)
                .await?;
            self.buffer.clear();

            info!(
                "call influx API took {} us, wrote {} packets.",
                t.elapsed().as_micros(),
                self.buffer_size
            );
        }
        Ok(())
    }
}
