use anyhow::{bail, Context, Result};
use biliapi::ws_protocol::{magic::KnownOperation, Operation, Packet};
use chrono::{DateTime, Local};
use influxdb_client::{Client as InfluxClient, TimestampOptions};
use tokio::sync::{broadcast, oneshot};
mod messages;
use messages::*;

pub struct InfluxAppender {
    client: InfluxClient,
    packets_receiver: broadcast::Receiver<Packet>,
}

impl InfluxAppender {
    pub fn new(client: InfluxClient, packets_receiver: broadcast::Receiver<Packet>) -> Self {
        Self {
            client,
            packets_receiver,
        }
    }

    pub async fn start(mut self, terminate_receiver: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = terminate_receiver => {
                info!("terminate.");
                Ok(())
            },
            r = self.start_writer() => {
                r
            }
        }
    }

    async fn start_writer(&mut self) -> Result<()> {
        while let Ok(packet) = self.packets_receiver.recv().await {
            self.process_packet(packet).await?;
        }
        bail!("Influx appender exited unexpectedly.")
    }

    async fn process_packet(&mut self, packet: Packet) -> Result<()> {
        let t = packet.time;
        let room_id = packet.room_id;
        match packet.operation {
            Operation::Known(KnownOperation::SendMsgReply) => {
                let msg = serde_json::from_str::<SendMsgReply>(&packet.body)
                    .context("转换 SendMsgReply 失败")?;

                trace!("msg cmd: {}", msg.cmd);
                self.on_send_msg_reply(msg, room_id, t).await
            }
            Operation::Known(KnownOperation::HeartbeatReply) => {
                let popularity: i64 = packet.body.parse()?;
                self.on_popularity(popularity, room_id, t).await
            }
            _ => Ok(()),
        }
    }

    async fn on_send_msg_reply(
        &self,
        msg: SendMsgReply,
        room_id: u64,
        t: DateTime<Local>,
    ) -> Result<()> {
        let point = match msg.cmd.as_str() {
            "SUPER_CHAT_MESSAGE" | "SUPER_CHAT_MESSAGE_JPN" => {
                let sc: SuperChat =
                    serde_json::from_value(msg.data).context("convert msg to super chat failed")?;
                sc.into_point(room_id, t)
            }
            "SEND_GIFT" => {
                let gift: SendGift =
                    serde_json::from_value(msg.data).context("convert msg to send gift failed")?;
                info!("gift: {:?}", gift);
                gift.into_point(room_id, t)
            }
            "USER_TOAST_MSG" => {
                let guard: UserToastMsg = serde_json::from_value(msg.data)
                    .context("convert msg to UserToastMsg failed.")?;
                info!("guard buy: {:?}", guard);
                guard.into_point(room_id, t)
            }
            "DANMU_MSG" => {
                // TODO: 统计弹幕
                return Ok(());
            }
            _ => return Ok(()),
        };
        self.client
            .insert_points(&[point], TimestampOptions::FromPoint)
            .await?;
        Ok(())
    }

    async fn on_popularity(&self, popularity: i64, room_id: u64, t: DateTime<Local>) -> Result<()> {
        let point = Popularity::new(popularity).into_point(room_id, t);
        self.client
            .insert_points(&[point], TimestampOptions::FromPoint)
            .await?;
        Ok(())
    }
}
