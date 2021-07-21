use influxdb_client::Point;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Deserialize)]
struct UserInfo {
    uname: String,
}

// super chat
#[derive(Debug, Deserialize)]
pub struct SuperChat {
    // rmb
    price: u32,
    //
    #[serde(rename = "uid", deserialize_with = "super::u64_from_value")]
    sender_id: u64,

    user_info: UserInfo,
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
        let price = self.price();
        Point::new("live-gift")
            .tag("type", "superchat")
            .tag("gift_name", "superchat")
            .tag("sender", self.sender_id.to_string())
            .tag("sender_name", self.user_info.uname)
            .field("price", price)
    }
}
