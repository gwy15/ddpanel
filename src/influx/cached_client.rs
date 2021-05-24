use anyhow::Result;
use influxdb_client::{Client as InfluxClient, Point, TimestampOptions};
use parking_lot::Mutex;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::time::sleep;

const DEFAULT_CACHE_SIZE: usize = 32;

pub struct CachedInfluxClient {
    client: InfluxClient,

    /// 从开始到现在成功插入的数量
    insert_count: u64,

    /// 插入失败的数据点的数量
    fail_count: Arc<Mutex<u64>>,

    /// 保存的 buffer 大小
    buffered_points: Vec<Point>,
    /// buffer 最大大小
    buffer_size: usize,

    /// 是否后台执行插入，默认 true
    async_write: bool,
}

impl CachedInfluxClient {
    pub fn new(client: InfluxClient) -> Self {
        Self {
            insert_count: 0,
            fail_count: Default::default(),
            client,
            buffered_points: vec![],
            buffer_size: DEFAULT_CACHE_SIZE,
            async_write: true,
        }
    }

    pub fn buffer_size(mut self, buffer_size: usize) -> Self {
        self.buffer_size = buffer_size;
        self
    }

    #[allow(unused)]
    pub fn async_write(mut self, async_write: bool) -> Self {
        self.async_write = async_write;
        self
    }

    /// 向 influxdb 插入一个数据点
    pub async fn insert_point(&mut self, point: Point) -> Result<()> {
        self.insert_count += 1;

        self.buffered_points.push(point);

        if self.buffered_points.len() >= self.buffer_size {
            self.flush().await?;
        }

        if self.insert_count % 100 == 0 {
            debug!("{} points inserted to influxdb.", self.insert_count);
        }
        Ok(())
    }

    /// 在 batch 模式下立刻向 influxdb 写入所有缓存的数据点
    pub async fn flush(&mut self) -> Result<()> {
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
                    if i > 0 {
                        info!(
                            "insert influx success, took {} ms, wrote {} points. (previously {} retries)",
                            t.elapsed().as_millis(),
                            points.len(),
                            i
                        );
                    } else {
                        info!(
                            "insert influx success, took {} ms, wrote {} points.",
                            t.elapsed().as_millis(),
                            points.len(),
                        );
                    }
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

impl Drop for CachedInfluxClient {
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
