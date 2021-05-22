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
                "Gift {{ {} * {} = ￥{}}}",
                self.gift_name,
                self.num,
                self.price()
            ))
        } else {
            f.write_fmt(format_args!("Gift {{ {} * {} }}", self.gift_name, self.num))
        }
    }
}
impl super::ToPoint for SendGift {
    fn into_basic_point(self) -> Point {
        if !self.is_free() {
            Point::new("live-gift")
                .tag("type", "gift")
                .tag("gift_name", self.gift_name.as_str())
                .tag("sender", self.sender_id as i64)
                .field("num", self.num as i64)
                .field("price", self.price())
        } else {
            Point::new("live-gift")
                .tag("type", "free")
                .tag("gift_name", self.gift_name.as_str())
                .field("num", self.num as i64)
                .field("coin", self.price_milli as f64)
        }
    }
}
