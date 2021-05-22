use anyhow::Result;
use biliapi::ws_protocol::Packet;
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{
    fs::{File, OpenOptions},
    io::{AsyncWriteExt, BufWriter},
    sync::{broadcast, oneshot},
};

/// 两秒写一次数据
const MAX_FLUSH: Duration = Duration::from_secs(2);

pub struct FileAppender {
    count: u64,
    file: BufWriter<File>,
    receiver: broadcast::Receiver<Packet>,
}

impl FileAppender {
    pub async fn new(path: PathBuf, receiver: broadcast::Receiver<Packet>) -> Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        let file = BufWriter::new(file);
        Ok(Self {
            count: 0,
            file,
            receiver,
        })
    }

    pub async fn start(mut self, terminate_receiver: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = terminate_receiver => {
                info!("terminate.");
                self.file.flush().await?;
                Ok(())
            },
            r = self.start_writer() => {
                self.file.flush().await?;
                r
            }
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
                        self.file.flush().await?;
                        last_flush = Instant::now();
                    }
                }
                Err(e) => {
                    error!("file appender recv error: {:?}", e);
                    return Err(e.into());
                }
            }
        }
    }

    async fn write_packet(&mut self, packet: Packet) -> Result<()> {
        self.count += 1;
        self.file
            .write_all(serde_json::to_string(&packet)?.as_bytes())
            .await?;
        self.file.write_all("\n".as_bytes()).await?;

        if self.count % 10_000 == 0 {
            info!("file recorded {} packets.", self.count);
        }

        Ok(())
    }
}
