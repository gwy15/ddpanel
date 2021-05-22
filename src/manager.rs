//! 总经理

use std::{collections::HashMap, path::PathBuf};

use anyhow::{bail, Result};
use biliapi::ws_protocol::Packet;
use influxdb_client::Client as InfluxClient;
use parking_lot::RwLock;
use tokio::sync::{broadcast, oneshot};

use crate::{
    file_appender::FileAppender,
    influx::InfluxAppender,
    monitor::Monitor,
    replayer::FileReplayer,
    task_factory::{TaskFactory, TaskSet},
};

// FIXME: 这是一个丑陋的打洞实现，需要修改为更好的实现
lazy_static::lazy_static! {
    pub static ref ROOM_ID_TO_STREAMER: RwLock<HashMap<u64, String>> = Default::default();
}

pub struct Manager {
    packet_channel: broadcast::Sender<Packet>,

    packet_producers: HashMap<u64, oneshot::Sender<()>>,

    packet_subscribers: Vec<oneshot::Sender<()>>,
}

impl Manager {
    pub fn new() -> Self {
        let (packet_sender, _) = broadcast::channel::<Packet>(10_000);

        Self {
            packet_channel: packet_sender,
            packet_producers: HashMap::new(),
            packet_subscribers: vec![],
        }
    }

    /// add file appender (consumer)
    pub async fn file_appender(mut self, path: PathBuf) -> Result<Self> {
        let receiver = self.packet_channel.subscribe();
        let appender = FileAppender::new(path, receiver).await?;

        let (terminate_tx, terminate_rx) = oneshot::channel();
        self.packet_subscribers.push(terminate_tx);
        tokio::spawn(appender.start(terminate_rx));

        Ok(self)
    }

    pub fn influx_appender(mut self, influx_client: InfluxClient) -> Self {
        let receiver = self.packet_channel.subscribe();
        let appender = InfluxAppender::new(influx_client, receiver);

        let (terminate_tx, terminate_rx) = oneshot::channel();
        self.packet_subscribers.push(terminate_tx);
        tokio::spawn(appender.start(terminate_rx));

        self
    }

    pub fn no_appender(self) -> Self {
        let receiver = self.packet_channel.subscribe();
        tokio::spawn(async move {
            let mut receiver = receiver;
            loop {
                match receiver.recv().await {
                    Ok(packet) => {
                        // tokio::time::sleep(std::time::Duration::from_micros(1)).await;
                        std::mem::drop(packet)
                    }
                    Err(e) => {
                        error!("no-op receiver recv error: {:?}", e);
                    }
                }
            }
        });
        self
    }

    /// run with task factory
    pub async fn start(mut self, task_file: PathBuf) -> Result<()> {
        let http_client = biliapi::connection::new_client()?;

        let mut task_receiver = TaskFactory::start(task_file);
        while let Some(tasks) = task_receiver.recv().await {
            // check for tasks
            let cur_tasks: TaskSet = self.packet_producers.keys().cloned().collect();
            // terminate old tasks
            let stop_tasks = cur_tasks.difference(&tasks);
            for stop_id in stop_tasks {
                info!("Stopping monitor room {}", stop_id);
                // safety: stop_id 一定在 cur_tasks 中
                let sender = self.packet_producers.remove(stop_id).unwrap();
                if sender.send(()).is_err() {
                    bail!(
                        "Send terminate to id {} but the monitor is already dead.",
                        stop_id
                    );
                };
            }
            // start new tasks
            let new_tasks = tasks.difference(&cur_tasks);
            for &new_id in new_tasks {
                info!("start new monitor room: {}", new_id);
                let (terminate_sender, terminate_receiver) = oneshot::channel();
                let monitor =
                    Monitor::new(new_id, self.packet_channel.clone(), http_client.clone());
                tokio::spawn(monitor.start(terminate_receiver));
                self.packet_producers.insert(new_id, terminate_sender);
            }
        }
        Ok(())
    }

    pub async fn replay(self, replay_file: PathBuf) -> Result<()> {
        let http_client = biliapi::connection::new_client()?;

        let replayer =
            FileReplayer::new(replay_file, self.packet_channel.clone(), http_client).await?;

        replayer.start().await?;

        Ok(())
    }
}

impl Drop for Manager {
    fn drop(&mut self) {
        let packet_subscribers = std::mem::take(&mut self.packet_subscribers);
        packet_subscribers.into_iter().for_each(|tx| {
            tx.send(()).ok();
        });
    }
}
