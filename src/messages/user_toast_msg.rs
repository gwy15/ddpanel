use influxdb_client::Point;

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
impl super::ToPoint for UserToastMsg {
    fn into_basic_point(self) -> Point {
        Point::new("live-gift")
            .tag("type", "guard")
            .tag("gift_name", self.gift_name.as_str())
            .field("num", self.num as i64)
            .field("price", self.price())
    }
}
