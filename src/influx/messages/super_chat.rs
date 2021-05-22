use influxdb_client::Point;
use std::fmt::{self, Display, Formatter};

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
impl Display for SuperChat {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!("SuperChat {{ ￥{} }}", self.price))
    }
}
impl super::ToPoint for SuperChat {
    fn into_basic_point(self) -> Point {
        Point::new("live-gift")
            .tag("type", "superchat")
            .tag("gift_name", "superchat")
            .tag("sender", self.sender_id as i64)
            .field("price", self.price())
    }
}
