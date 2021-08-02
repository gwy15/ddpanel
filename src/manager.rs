//! 总经理

use std::{collections::HashMap, path::PathBuf};

use anyhow::Result;
use biliapi::ws_protocol::Packet;
use influxdb_client::Client as InfluxClient;
use parking_lot::RwLock;
use tokio::sync::{broadcast, oneshot, watch};

use crate::{
    file_appender::FileAppender,
    influx::InfluxAppender,
    monitor::Monitor,
    replayer::FileReplayer,
    spider::SpiderInfo,
    task_factory::{TaskFactory, TaskSet},
};

// FIXME: 这是一个丑陋的打洞实现，需要修改为更好的实现
lazy_static::lazy_static! {
    pub static ref ROOM_ID_TO_STREAMER: RwLock<HashMap<u64, String>> = Default::default();
}

pub struct Manager {
    /// 主通信渠道
    packet_channel: broadcast::Sender<Packet>,
    /// 接收到结束信号的时候，会向各个 monitor 发送结束信号
    monitor_terminate_senders: HashMap<u64, oneshot::Sender<()>>,

    /// 等待各个 subscriber 结束的 handler
    subscriber_handlers: Vec<tokio::task::JoinHandle<Result<()>>>,

    /// 爬虫停止
    spider_stop: Option<oneshot::Sender<()>>,
    /// 爬虫任务发布
    spider_tasks_channel: (watch::Sender<TaskSet>, watch::Receiver<TaskSet>),
    /// 爬虫信息通道
    spider_channel: broadcast::Sender<SpiderInfo>,
}

impl Manager {
    pub fn new() -> Self {
        let (packet_sender, _) = broadcast::channel::<Packet>(10_000);
        let (spider_channel, _) = broadcast::channel::<SpiderInfo>(1_000);
        let spider_tasks_channel = watch::channel(Default::default());

        Self {
            packet_channel: packet_sender,
            monitor_terminate_senders: HashMap::new(),
            subscriber_handlers: vec![],
            spider_stop: None,
            spider_tasks_channel,
            spider_channel,
        }
    }

    /// add file appender (consumer)
    pub async fn file_appender(mut self, live_path: String, bili_path: String) -> Result<Self> {
        let receiver = self.packet_channel.subscribe();
        let appender = FileAppender::new(live_path, receiver).await?;
        let handler = tokio::spawn(appender.start());
        self.subscriber_handlers.push(handler);

        let receiver = self.spider_channel.subscribe();
        let appender = FileAppender::new(bili_path, receiver).await?;
        let handler = tokio::spawn(appender.start());
        self.subscriber_handlers.push(handler);

        Ok(self)
    }

    /// 增加一个 influx 插入，buffer size >0 才有效
    pub fn influx_appender(mut self, influx_client: InfluxClient, buffer_size: usize) -> Self {
        let packets_receiver = self.packet_channel.subscribe();
        let spider_receiver = self.spider_channel.subscribe();
        let mut appender = InfluxAppender::new(influx_client, packets_receiver, spider_receiver);
        if buffer_size > 0 {
            appender = appender.buffer_size(buffer_size);
        }

        let handler = tokio::spawn(appender.start());
        self.subscriber_handlers.push(handler);

        self
    }

    pub fn no_appender(mut self) -> Self {
        let receiver = self.packet_channel.subscribe();
        let handler = tokio::spawn(async move {
            let mut receiver = receiver;
            loop {
                match receiver.recv().await {
                    Ok(packet) => {
                        // tokio::time::sleep(std::time::Duration::from_micros(1)).await;
                        std::mem::drop(packet)
                    }
                    Err(e) => {
                        info!("no-op receiver close: {:?}", e);
                        return Ok(());
                    }
                }
            }
        });
        self.subscriber_handlers.push(handler);
        self
    }

    /// run with task factory, never end
    pub async fn start(mut self, task_file: PathBuf, cookie_path: PathBuf) -> Result<()> {
        let http_client = biliapi::connection::new_client()?;

        let mut task_receiver = TaskFactory::start(task_file);

        let spider = crate::spider::Spider::new(
            self.spider_tasks_channel.1.clone(),
            self.spider_channel.clone(),
            cookie_path,
        );
        let (tx, rx) = oneshot::channel();
        tokio::spawn(spider.start(rx));
        self.spider_stop = Some(tx);

        // 一直等到 task_receiver 结束
        loop {
            let recv = task_receiver.recv().await;
            match recv {
                Some(tasks) => {
                    let live_rooms = tasks.live_rooms;

                    // check for tasks
                    let cur_rooms: TaskSet =
                        self.monitor_terminate_senders.keys().cloned().collect();
                    // terminate old tasks
                    let stop_rooms = cur_rooms.difference(&live_rooms);
                    for stop_id in stop_rooms {
                        info!("Stopping monitor room {}", stop_id);
                        // safety: stop_id 一定在 cur_tasks 中
                        let sender = self.monitor_terminate_senders.remove(stop_id).unwrap();
                        if sender.send(()).is_err() {
                            error!(
                                "Send terminate to id {} but the monitor is already dead.",
                                stop_id
                            );
                        };
                    }
                    // start new tasks
                    let new_rooms = live_rooms.difference(&cur_rooms);
                    for &new_id in new_rooms {
                        info!("start new monitor room: {}", new_id);
                        let (terminate_sender, terminate_receiver) = oneshot::channel();
                        let monitor =
                            Monitor::new(new_id, self.packet_channel.clone(), http_client.clone());
                        tokio::spawn(monitor.start(terminate_receiver));
                        self.monitor_terminate_senders
                            .insert(new_id, terminate_sender);
                    }

                    // send to spider
                    match self.spider_tasks_channel.0.send(tasks.users) {
                        Ok(_) => {}
                        Err(e) => {
                            error!("failed to send tasks on spider tasks chanel: {:?}", e);
                        }
                    }
                }
                None => {
                    debug!("manger noticed that task factory has stopped.");
                    break;
                }
            }
        }

        self.finish().await?;

        Ok(())
    }

    pub async fn replay(self, replay_file: String, replay_delay_ms: u32) -> Result<()> {
        let http_client = biliapi::connection::new_client()?;

        let mut replayer =
            FileReplayer::new(self.packet_channel.clone(), http_client, replay_delay_ms).await?;

        replayer.replay(replay_file).await?;
        std::mem::drop(replayer);

        info!("replay finished, waiting for all handlers to finish.");

        self.finish().await?;

        // 异步模式再等一秒，等异步插入结束
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;

        Ok(())
    }

    pub async fn finish(self) -> Result<()> {
        if let Some(t) = self.spider_stop {
            if let Err(e) = t.send(()) {
                warn!("failed to stop spider: {:?}", e);
            }
        }

        for (_id, terminator) in self.monitor_terminate_senders.into_iter() {
            if terminator.send(()).is_err() {
                warn!("A monitor has already died.");
            };
        }

        // drop the packet sender so that all receivers can stop
        std::mem::drop(self.packet_channel);
        std::mem::drop(self.spider_channel);

        info!(
            "manger waiting for all processors to finish ({} processors)",
            self.subscriber_handlers.len()
        );
        for h in self.subscriber_handlers {
            info!("waiting for handler to stop: {:?}", h);
            match h.await {
                Ok(r) => {
                    info!("subscriber join result: {:?}", r);
                }
                Err(e) => {
                    error!("failed to join handler: {:?}", e);
                }
            }
        }

        info!("manager graceful stop success.");

        Ok(())
    }
}
