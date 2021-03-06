use anyhow::Result;
use async_compression::tokio::write::GzipEncoder;
use chrono::{Date, Utc};
use chrono_tz::{Asia::Shanghai, Tz};
use serde::Serialize;
use std::{
    pin::Pin,
    time::{Duration, Instant},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWrite, AsyncWriteExt, BufWriter},
    sync::broadcast::{self, error::RecvError},
};

/// 两秒写一次数据
const MAX_FLUSH: Duration = Duration::from_secs(2);

type Writer = Pin<Box<dyn AsyncWrite + Send>>;

pub struct FileAppender<T> {
    count: u64,
    path: String,
    writer: Writer,
    writer_date: Date<Tz>,
    receiver: broadcast::Receiver<T>,
}

impl<T: Serialize + Clone> FileAppender<T> {
    pub async fn new(path: String, receiver: broadcast::Receiver<T>) -> Result<Self> {
        let (writer, date) = Self::make_writer(&path).await?;
        Ok(Self {
            count: 0,
            path,
            writer,
            writer_date: date,
            receiver,
        })
    }

    pub async fn start(mut self) -> Result<()> {
        let r = self.start_writer().await;
        debug!("file appender {} closing.", self.path);
        self.writer.flush().await?;
        self.writer.shutdown().await?;
        info!("file {} appender closed.", self.path);
        r
    }

    async fn make_writer(path: &str) -> Result<(Writer, Date<Tz>)> {
        let date = Utc::today().with_timezone(&Shanghai);
        let path = path.replace("%", &date.format("%Y-%m-%d").to_string());
        info!("file will be written to {}", path);
        let file: File = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)
            .await?;
        let writer = BufWriter::new(file);
        if path.ends_with(".gz") {
            info!("file will be encoded in gzip");
            let writer = GzipEncoder::new(writer);
            let writer: Writer = Box::pin(writer);
            Ok((writer, date))
        } else {
            let writer: Writer = Box::pin(writer);
            Ok((writer, date))
        }
    }

    async fn start_writer(&mut self) -> Result<()> {
        let mut items: u64 = 0;
        let mut last_flush = Instant::now();

        loop {
            match self.receiver.recv().await {
                Ok(packet) => {
                    self.write_packet(packet).await?;
                    items += 1;
                    if items % 1_000 == 0 || Instant::now().duration_since(last_flush) > MAX_FLUSH {
                        self.flush_and_swap().await?;
                        last_flush = Instant::now();
                    }
                }
                Err(RecvError::Lagged(i)) => {
                    error!("File appender lagged! lagged {} packets", i);
                    continue;
                }
                Err(RecvError::Closed) => {
                    info!("file appender received stop signal. stopping");
                    return Ok(());
                }
            }
        }
    }

    async fn write_packet(&mut self, packet: T) -> Result<()> {
        self.count += 1;
        self.writer
            .write_all(serde_json::to_string(&packet)?.as_bytes())
            .await?;
        self.writer.write_all("\n".as_bytes()).await?;

        if self.count % 10_000 == 0 {
            info!("file recorded {} packets.", self.count);
        }

        Ok(())
    }

    async fn flush_and_swap(&mut self) -> Result<()> {
        self.writer.flush().await?;
        if Utc::today().with_timezone(&Shanghai) != self.writer_date {
            self.writer.shutdown().await?;
            let (writer, date) = Self::make_writer(&self.path).await?;
            self.writer = writer;
            self.writer_date = date;
        }
        Ok(())
    }
}
