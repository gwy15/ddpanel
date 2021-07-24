use chrono::{DateTime, Local};
use std::fmt::{self, Display, Formatter};

use influxdb_client::Point;

#[derive(Debug, Deserialize)]
pub struct SendGift {
    coin_type: String,
    #[serde(rename = "giftName")]
    gift_name: String,

    // 瓜子
    #[serde(rename = "price")]
    price_milli: u32,

    #[serde(rename = "num")]
    num: u32,

    #[serde(rename = "uid", deserialize_with = "super::u64_from_value")]
    sender_id: u64,

    #[serde(rename = "uname")]
    sender_name: String,

    /// 现在可以在某个直播间送给另一个主播礼物
    #[serde(default, rename = "send_master")]
    gift_receiver: Option<GiftReceiver>,
}
/// 现在可以在某个直播间送给另一个主播礼物
#[derive(Debug, Deserialize, Clone)]
pub struct GiftReceiver {
    room_id: u64,
    uid: u64,
    uname: String,
}

impl SendGift {
    pub fn price(&self) -> f64 {
        if self.coin_type == "gold" {
            (self.price_milli * self.num) as f64 * 0.001
        } else {
            0f64
        }
    }
    pub fn is_free(&self) -> bool {
        self.coin_type == "silver"
    }
}
impl Display for SendGift {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        if self.coin_type == "gold" {
            f.write_fmt(format_args!(
                "Gift {{ {} * {} = ￥{}",
                self.gift_name,
                self.num,
                self.price()
            ))?;
            if let Some(receiver) = &self.gift_receiver {
                f.write_fmt(format_args!(" => {}", receiver.uname))?;
            }
            f.write_str(" }")
        } else {
            f.write_fmt(format_args!("Gift {{ {} * {} }}", self.gift_name, self.num))
        }
    }
}
impl super::ToPoint for SendGift {
    fn into_point(self, room_info: &crate::influx::RoomInfo, t: DateTime<Local>) -> Point {
        let receiver = self.gift_receiver.clone();
        let pt = self.into_basic_point();
        match receiver {
            Some(receiver) => pt
                .tag("room_id", receiver.room_id.to_string())
                .tag("streamer", receiver.uname),
            None => pt
                .tag("room_id", room_info.id.to_string())
                .tag("streamer", room_info.streamer.as_str()),
        }
        .timestamp(t.timestamp_millis())
    }

    fn into_basic_point(self) -> Point {
        if !self.is_free() {
            let price = self.price();
            Point::new("live-gift")
                .tag("type", "gift")
                .tag("gift_name", self.gift_name)
                .tag("sender", self.sender_id.to_string())
                .tag("sender_name", self.sender_name.as_str())
                .field("num", self.num as i64)
                .field("price", price)
        } else {
            Point::new("live-gift")
                .tag("type", "free")
                .tag("gift_name", self.gift_name)
                .field("num", self.num as i64)
                .field("coin", self.price_milli as f64)
        }
    }
}
