use biliapi::requests::{UploaderStat, UserInfo};
use chrono::{DateTime, Utc};
use influxdb_client::Point;
use serde::{Deserialize, Serialize};

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
