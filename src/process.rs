use anyhow::{Context, Result};
use biliapi::ws_protocol::{magic::KnownOperation, Operation};
use chrono::{DateTime, Local};
use influxdb_client::{Client as InfluxClient, TimestampOptions};

use super::messages::*;

/// 把 packet 转换为 message
pub async fn on_packet(
    packet: biliapi::ws_protocol::Packet,
    room_id: u64,
    influx: &InfluxClient,
) -> Result<()> {
    let t = packet.time;
    match packet.operation {
        Operation::Known(KnownOperation::SendMsgReply) => {
            let msg = serde_json::from_str::<crate::Message>(&packet.body)
                .context("转换 SendMsgReply 为 crate::Message 失败")?;

            debug!("msg cmd: {}", msg.cmd);
            on_message(msg, room_id, t, influx).await
        }
        Operation::Known(KnownOperation::HeartbeatReply) => {
            // TODO: 人气值
            Ok(())
        }
        _ => Ok(()),
    }
}

async fn on_message(
    msg: crate::Message,
    room_id: u64,
    t: DateTime<Local>,
    influx: &InfluxClient,
) -> Result<()> {
    let point = match msg.cmd.as_str() {
        "SUPER_CHAT_MESSAGE" => {
            let sc: SuperChat =
                serde_json::from_value(msg.data).context("convert msg to super chat failed")?;
            sc.to_point(room_id, t)
        }
        "SEND_GIFT" => {
            let gift: SendGift =
                serde_json::from_value(msg.data).context("convert msg to send gift failed")?;
            info!("gift: {:?}", gift);
            gift.to_point(room_id, t)
        }
        "USER_TOAST_MSG" => {
            let guard: UserToastMsg =
                serde_json::from_value(msg.data).context("convert msg to UserToastMsg failed.")?;
            info!("guard buy: {:?}", guard);
            guard.to_point(room_id, t)
        }
        _ => return Ok(()),
    };
    influx
        .insert_points(&[point], TimestampOptions::FromPoint)
        .await?;
    Ok(())
}
