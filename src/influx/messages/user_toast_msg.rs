use influxdb_client::Point;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Deserialize)]
pub struct UserToastMsg {
    #[serde(rename = "uid")]
    sender_id: u64,

    #[serde(rename = "username")]
    sender_name: String,

    /// 金瓜子，但是是总价，会随着 num 变，离谱
    #[serde(rename = "price")]
    price_milli: u32,

    #[serde(rename = "role_name")]
    gift_name: String,

    num: u32,
}
impl UserToastMsg {
    pub fn price(&self) -> f64 {
        (self.price_milli) as f64 * 0.001
    }
}
impl Display for UserToastMsg {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_fmt(format_args!(
            "UserToastMsg {{ {} * {} = ￥{} }}",
            self.gift_name,
            self.num,
            self.price()
        ))
    }
}
impl super::ToPoint for UserToastMsg {
    fn into_basic_point(self) -> Point {
        let price = self.price();
        Point::new("live-gift")
            .tag("type", "guard")
            .tag("gift_name", self.gift_name)
            .tag("sender", self.sender_id.to_string())
            .tag("sender_name", self.sender_name.as_str())
            .field("num", self.num as i64)
            .field("price", price)
    }
}
