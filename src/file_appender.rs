use anyhow::Result;
use biliapi::ws_protocol::Packet;
use chrono::{Date, Local};
use std::time::{Duration, Instant};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::broadcast::{self, error::RecvError},
};

/// 两秒写一次数据
const MAX_FLUSH: Duration = Duration::from_secs(2);

pub struct FileAppender {
    count: u64,
    path: String,
    writer: BufWriter<File>,
    writer_date: Date<Local>,
    receiver: broadcast::Receiver<Packet>,
}

impl FileAppender {
    pub async fn new(path: String, receiver: broadcast::Receiver<Packet>) -> Result<Self> {
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
        info!("file appender flushing.");
        self.flush_and_swap().await?;
        r
    }

    async fn make_writer(path: &str) -> Result<(BufWriter<File>, Date<Local>)> {
        let date = Local::today();
        let path = path.replace("%", &date.format("%Y-%m-%d").to_string());
        info!("file will be written to {}", path);
        let file = OpenOptions::new()
            .write(true)
            .append(true)
            .create(true)
            .open(&path)
            .await?;
        let writer = BufWriter::new(file);
        Ok((writer, date))
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

    async fn write_packet(&mut self, packet: Packet) -> Result<()> {
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
        if Local::today() != self.writer_date {
            let (writer, date) = Self::make_writer(&self.path).await?;
            self.writer = writer;
            self.writer_date = date;
        }
        Ok(())
    }
}
