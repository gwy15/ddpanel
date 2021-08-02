use std::time::Duration;

use anyhow::*;
use biliapi::{
    requests::{UploaderStat, UserInfo},
    Request,
};
use chrono::{DateTime, Utc};
use influxdb_client::Point;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, oneshot, watch};

use crate::task_factory::TaskSet;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct SpiderInfo {
    pub username: String,
    pub uid: u64,
    pub time: DateTime<Utc>,
    pub data: SpiderData,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SpiderData {
    UploaderStat(UploaderStat),
    UserInfo(UserInfo),
}

impl SpiderInfo {
    pub fn into_point(self) -> Point {
        let pt = Point::new("bili-info")
            .tag("uploader", self.username)
            .timestamp(self.time.timestamp_millis());
        match self.data {
            SpiderData::UploaderStat(stat) => pt
                .field("video_views", stat.video_views as f64)
                .field("article_views", stat.article_views as f64)
                .field("likes", stat.likes as f64),
            SpiderData::UserInfo(user_info) => pt.field("followers", user_info.followers as f64),
        }
    }
}

pub struct Spider {
    tasks: watch::Receiver<TaskSet>,
    publish: broadcast::Sender<SpiderInfo>,
    client: reqwest::Client,
}

impl Spider {
    pub fn new(tasks: watch::Receiver<TaskSet>, publish: broadcast::Sender<SpiderInfo>) -> Self {
        // TODO: load client
        let client = reqwest::Client::new();
        Self {
            tasks,
            publish,
            client,
        }
    }

    pub async fn start(self, stop: oneshot::Receiver<()>) -> Result<()> {
        tokio::select! {
            _ = stop => {
                debug!("spider stopped");
                Ok(())
            },
            ret = self.start_spider() => {
                error!("spider stopped unexpectedly: {:?}", ret);
                ret
            }
        }
    }

    async fn start_spider(mut self) -> Result<()> {
        let mut batch_cd = tokio::time::interval(Duration::from_secs(3 * 60));
        batch_cd.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut user_cd = tokio::time::interval(Duration::from_secs(1));
        user_cd.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        tokio::time::sleep(Duration::from_secs(1)).await;
        info!("bilibili info spider started.");
        loop {
            batch_cd.tick().await;
            info!("fetch bilibili info in batch...");
            let tasks = self.tasks.borrow().clone();
            debug!("current batch tasks = {:?}", tasks);
            for user_id in tasks {
                user_cd.tick().await;
                if let Err(e) = self.process(user_id).await {
                    warn!("failed to process user info: {:?}", e);
                }
            }
        }
    }

    async fn process(&mut self, user_id: u64) -> Result<()> {
        debug!("get user info id={}", user_id);
        use biliapi::requests;
        let user_info = requests::UserInfo::request(&self.client, user_id).await?;
        let username = user_info.name.clone();
        self.publish.send(SpiderInfo {
            username: username.clone(),
            uid: user_id,
            time: Utc::now(),
            data: SpiderData::UserInfo(user_info.clone()),
        })?;

        debug!("get user uploader stat {} id={}", username, user_id);
        let up_info = requests::UploaderStat::request(&self.client, user_id).await?;
        self.publish.send(SpiderInfo {
            username: username.clone(),
            uid: user_id,
            time: Utc::now(),
            data: SpiderData::UploaderStat(up_info),
        })?;
        info!("user info {} (id={}) processed.", username, user_id);

        Ok(())
    }
}
