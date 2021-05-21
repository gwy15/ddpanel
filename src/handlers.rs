use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use biliapi::ws_protocol::{magic::KnownOperation, Operation, Packet};
use chrono::{DateTime, Local};
use influxdb_client::{Client as InfluxClient, TimestampOptions};
use tokio::{
    io::AsyncWriteExt,
    sync::mpsc::{Receiver, Sender},
};

use super::messages::*;

pub async fn start_file_appender(path: PathBuf, mut rx: Receiver<Packet>) -> anyhow::Result<()> {
    let file = tokio::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .append(true)
        .open(&path)
        .await?;
    let mut file = tokio::io::BufWriter::new(file);

    let mut items: u32 = 0;
    let mut last_flush = Instant::now();

    const MAX_FLUSH: Duration = Duration::from_secs(60);
    loop {
        while let Some(packet) = rx.recv().await {
            file.write_all(serde_json::to_string(&packet)?.as_bytes())
                .await?;
            file.write_all("\n".as_bytes()).await?;
            items += 1;
            if items % 1_000 == 0 || Instant::now().duration_since(last_flush) > MAX_FLUSH {
                file.flush().await?;
                last_flush = Instant::now();
            }
        }
    }
}

/// 把 packet 转换为 message
pub async fn on_packet(
    packet: biliapi::ws_protocol::Packet,
    room_id: u64,
    influx: &InfluxClient,
    tx: &Sender<Packet>,
) -> Result<()> {
    tx.send(packet.clone()).await?;
    let t = packet.time;
    match packet.operation {
        Operation::Known(KnownOperation::SendMsgReply) => {
            let msg = serde_json::from_str::<SendMsgReply>(&packet.body)
                .context("转换 SendMsgReply 失败")?;

            debug!("msg cmd: {}", msg.cmd);
            on_send_msg_reply(msg, room_id, t, influx).await
        }
        Operation::Known(KnownOperation::HeartbeatReply) => {
            let popularity: i64 = packet.body.parse()?;
            on_popularity(popularity, room_id, t, influx).await
        }
        _ => Ok(()),
    }
}

async fn on_send_msg_reply(
    msg: SendMsgReply,
    room_id: u64,
    t: DateTime<Local>,
    influx: &InfluxClient,
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
            let guard: UserToastMsg =
                serde_json::from_value(msg.data).context("convert msg to UserToastMsg failed.")?;
            info!("guard buy: {:?}", guard);
            guard.into_point(room_id, t)
        }
        "DANMU_MSG" => {
            // TODO: 统计弹幕

            return Ok(());
        }
        _ => return Ok(()),
    };
    influx
        .insert_points(&[point], TimestampOptions::FromPoint)
        .await?;
    Ok(())
}

async fn on_popularity(
    popularity: i64,
    room_id: u64,
    t: DateTime<Local>,
    influx: &InfluxClient,
) -> Result<()> {
    let point = Popularity::new(popularity).into_point(room_id, t);
    influx
        .insert_points(&[point], TimestampOptions::FromPoint)
        .await?;
    Ok(())
}
