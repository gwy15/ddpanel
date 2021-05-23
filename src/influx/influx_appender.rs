use anyhow::{Context, Result};
use biliapi::ws_protocol::{KnownOperation, Operation, Packet};
use chrono::{DateTime, Local};
use influxdb_client::{Client as InfluxClient, Point, TimestampOptions};
use parking_lot::Mutex;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{
        broadcast::{self, error::RecvError},
        oneshot,
    },
    time::sleep,
};

const MAX_CACHE_TIME: Duration = Duration::from_secs(2);
const DEFAULT_CACHE_SIZE: usize = 32;

use super::{messages::*, RoomInfo};

/// 解析接收到的 packet，向 influxdb 插入
///
/// 提供一个 async_writer 选项，在此模式下会将写入放到后台执行
///
/// TODO: 拆分处理逻辑和数据库逻辑
pub struct InfluxAppender {
    /// 从开始到现在成功插入的数量
    insert_count: u64,

    /// 插入失败的数据点的数量
    fail_count: Arc<Mutex<u64>>,

    /// 保存的 buffer 大小
    buffered_points: Vec<Point>,
    /// buffer 最大大小
    buffer_size: usize,

    client: InfluxClient,
    /// 接收 packet
    packets_receiver: broadcast::Receiver<Packet>,

    /// 是否后台执行插入，默认 true
    async_write: bool,

    last_flush_time: Instant,
}

impl InfluxAppender {
    pub fn new(client: InfluxClient, packets_receiver: broadcast::Receiver<Packet>) -> Self {
        Self {
            insert_count: 0,
            fail_count: Default::default(),
            client,
            buffered_points: vec![],
            buffer_size: DEFAULT_CACHE_SIZE,
            packets_receiver,
            async_write: true,
            last_flush_time: Instant::now(),
        }
    }

    pub fn buffer(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    #[allow(unused)]
    pub fn async_write(mut self, async_write: bool) -> Self {
        self.async_write = async_write;
        self
    }

    pub async fn start(mut self, terminate_receiver: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = terminate_receiver => {
                info!("received terminate signal. terminate influx client.");
                self.flush().await?;
                Ok(())
            },
            _ = self.start_writer() => {
                info!("the channel is closed. flushing remaining caches");
                self.flush().await?;
                Ok(())
            }
        }
    }

    async fn start_writer(&mut self) {
        // https://play.rust-lang.org/?version=stable&mode=debug&edition=2018&gist=6ba08f53a430171c7791ac5d0dd15a84
        loop {
            match self.packets_receiver.recv().await {
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
        self.insert_point(point).await?;
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
        self.insert_point(point).await?;
        Ok(())
    }

    /// 向 influxdb 插入一个数据点
    async fn insert_point(&mut self, point: Point) -> Result<()> {
        self.insert_count += 1;

        self.buffered_points.push(point);

        let should_flush = Instant::now().saturating_duration_since(self.last_flush_time)
            > MAX_CACHE_TIME
            || self.buffered_points.len() >= self.buffer_size;

        if should_flush {
            self.flush().await?;
        }

        if self.insert_count % 100 == 0 {
            debug!("{} points inserted to influxdb.", self.insert_count);
        }
        Ok(())
    }

    /// 在 batch 模式下立刻向 influxdb 写入所有缓存的数据点
    async fn flush(&mut self) -> Result<()> {
        self.last_flush_time = Instant::now();
        if self.buffered_points.is_empty() {
            return Ok(());
        }
        let points = std::mem::take(&mut self.buffered_points);
        info!(
            "flushing {} points. async: {}",
            points.len(),
            self.async_write
        );
        if self.async_write {
            let client = self.client.clone();
            let fail_count = self.fail_count.clone();
            let fut = async move {
                let points = points;
                if let Err(e) = Self::insert_points_retry_sync(&client, &points).await {
                    error!("async mode write failed: {:?}", e);
                    *fail_count.lock() += points.len() as u64;
                    // 异步模式下丢弃错误
                };
            };
            tokio::spawn(fut);
        } else {
            // sync mode
            if let Err(e) = Self::insert_points_retry_sync(&self.client, &points).await {
                *self.fail_count.lock() += points.len() as u64;
                return Err(e);
            };
        }

        Ok(())
    }

    /// 同步写入数据点，会重试三次
    async fn insert_points_retry_sync(client: &InfluxClient, points: &[Point]) -> Result<()> {
        let t = Instant::now();
        const RETRY_DELAY: [Duration; 3] = [
            Duration::from_secs(0),
            Duration::from_secs(1),
            Duration::from_secs(3),
        ];
        #[allow(clippy::needless_range_loop)]
        for i in 0..RETRY_DELAY.len() + 1 {
            match client
                .insert_points(points, TimestampOptions::FromPoint)
                .await
            {
                Ok(_) => {
                    info!(
                        "insert influx success, took {} ms, wrote {} points. (previously {} retries)",
                        t.elapsed().as_millis(),
                        points.len(),
                        i
                    );
                    return Ok(());
                }
                Err(e) => {
                    warn!("Error insert to influxdb: {:?}", e);
                    if i == RETRY_DELAY.len() {
                        error!("Insert to influx failed!");
                        return Err(e.into());
                    }
                    let delay = RETRY_DELAY[i];
                    info!("will retry after {} seconds.", delay.as_secs());
                    sleep(delay).await;
                }
            };
        }
        unreachable!()
    }
}

impl Drop for InfluxAppender {
    fn drop(&mut self) {
        info!(
            "influx appender wrote total of {} points.",
            self.insert_count
        );
        if !self.buffered_points.is_empty() {
            error!(
                "{} points still in buffer. This is a bug.",
                self.buffered_points.len()
            );
        }
        let fail_count = *self.fail_count.lock();
        if fail_count > 0 {
            error!(
                "Influx appender has totally lost {} points. You might want to replay.",
                fail_count
            );
        }
    }
}
