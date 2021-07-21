use crate::influx::RoomInfo;
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

    fn into_point(self, room_info: &RoomInfo, t: DateTime<Local>) -> Point {
        self.into_basic_point()
            .tag("room_id", room_info.id.to_string())
            .tag("streamer", room_info.streamer.as_str())
            .timestamp(t.timestamp_millis())
    }
}

pub fn u64_from_value<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::de::{Deserialize, Error, Unexpected};

    let v = Value::deserialize(deserializer)?;
    match v {
        Value::Null => {
            unimplemented!()
        }
        Value::Bool(b) => Err(Error::invalid_type(Unexpected::Bool(b), &"u64")),
        Value::Number(n) => match n.as_i64() {
            Some(n) => Ok(n as u64),
            None => Err(Error::custom(format!("Cannot parse number {} as u64", n))),
        },
        Value::String(s) => s
            .parse()
            .map_err(|e| Error::custom(format!("Failed to parse string {:?} as u64: {}", s, e))),
        Value::Array(_arr) => Err(Error::invalid_type(Unexpected::Seq, &"u64")),
        Value::Object(_obj) => Err(Error::invalid_type(Unexpected::Map, &"u64")),
    }
}

mod danmu;
mod popularity;
mod send_gift;
mod super_chat;
mod user_toast_msg;

pub use danmu::Danmu;
pub use popularity::Popularity;
pub use send_gift::SendGift;
pub use super_chat::SuperChat;
pub use user_toast_msg::UserToastMsg;
