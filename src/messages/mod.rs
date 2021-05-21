use chrono::{DateTime, Local};
use influxdb_client::Point;
use serde_json::Value;

/// SendMsgReply 对应的 message，大部分包都是这个类型
#[derive(Debug, Deserialize)]
pub struct SendMsgReply {
    pub cmd: String,
    #[serde(default = "Value::default")]
    pub data: Value,
}

pub trait ToPoint: Sized {
    fn into_basic_point(self) -> Point;

    fn into_point(self, room_id: u64, t: DateTime<Local>) -> Point {
        self.into_basic_point()
            .tag("room_id", room_id as i64)
            .timestamp(t.timestamp_millis())
    }
}

mod popularity;
mod send_gift;
mod super_chat;
mod user_toast_msg;

pub use popularity::Popularity;
pub use send_gift::SendGift;
pub use super_chat::SuperChat;
pub use user_toast_msg::UserToastMsg;
