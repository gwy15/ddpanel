use influxdb_client::Point;

// super chat
#[derive(Debug, Deserialize)]
pub struct SuperChat {
    // rmb
    price: u32,
    //
    #[serde(rename = "uid", deserialize_with = "super::u64_from_value")]
    sender_id: u64,
}
impl SuperChat {
    pub fn price(&self) -> f64 {
        self.price as f64
    }
}
impl super::ToPoint for SuperChat {
    fn into_basic_point(self) -> Point {
        Point::new("live-gift")
            .tag("type", "superchat")
            .tag("sender", self.sender_id as i64)
            .field("price", self.price())
    }
}
