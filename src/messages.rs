use chrono::{DateTime, Local};
use influxdb_client::Point;
use serde_json::Value;

/// SendMsgReply 对应的 message，大部分包都是这个类型
#[derive(Debug, Deserialize)]
pub struct Message {
    pub cmd: String,
    #[serde(default = "Value::default")]
    pub data: Value,
}

pub trait ToPoint: Sized {
    fn to_basic_point(self) -> Point;

    fn to_point(self, room_id: u64, t: DateTime<Local>) -> Point {
        self.to_basic_point()
            .tag("room_id", room_id as i64)
            .timestamp(t.timestamp_millis())
    }
}

// super chat
#[derive(Debug, Deserialize)]
pub struct SuperChat {
    // rmb
    price: u32,
    //
    #[serde(rename = "uid")]
    sender_id: u64,
}
impl SuperChat {
    pub fn price(&self) -> f64 {
        self.price as f64
    }
}
impl ToPoint for SuperChat {
    fn to_basic_point(self) -> Point {
        Point::new("bilibili")
            .tag("type", "superchat")
            .field("price", self.price())
    }
}

#[derive(Debug, Deserialize)]
pub struct SendGift {
    coin_type: String,
    #[serde(rename = "giftName")]
    gift_name: String,
    // 金瓜子
    #[serde(rename = "price")]
    price_milli: u32,

    #[serde(rename = "num")]
    num: u32,

    #[serde(rename = "uid")]
    sender_id: u64,
}
impl SendGift {
    pub fn price(&self) -> f64 {
        (self.price_milli * self.num) as f64 * 0.001
    }
}
impl ToPoint for SendGift {
    fn to_basic_point(self) -> Point {
        Point::new("bilibili")
            .tag("type", "gift")
            .tag("gift_name", self.gift_name.as_str())
            .field("price", self.price())
    }
}

#[derive(Debug, Deserialize)]
pub struct UserToastMsg {
    #[serde(rename = "uid")]
    sender_id: u64,
    #[serde(rename = "price")]
    price_milli: u32,

    #[serde(rename = "role_name")]
    gift_name: String,

    num: u32,
}
impl UserToastMsg {
    pub fn price(&self) -> f64 {
        (self.price_milli * self.num) as f64 * 0.001
    }
}
impl ToPoint for UserToastMsg {
    fn to_basic_point(self) -> Point {
        Point::new("bilibili")
            .tag("type", "guard")
            .tag("gift_name", self.gift_name.as_str())
            .field("price", self.price())
    }
}
