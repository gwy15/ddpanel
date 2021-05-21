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

    #[serde(rename = "uid")]
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
}
impl super::ToPoint for SendGift {
    fn into_basic_point(self) -> Point {
        if self.coin_type == "gold" {
            Point::new("live-gift")
                .tag("type", "gift")
                .tag("gift_name", self.gift_name.as_str())
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
