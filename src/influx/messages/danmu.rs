use influxdb_client::Point;

pub struct Danmu {
    value: u32,
}
impl Danmu {
    pub fn new(count: u32) -> Self {
        Self { value: count }
    }
}
impl super::ToPoint for Danmu {
    fn into_basic_point(self) -> Point {
        Point::new("live-popularity").field("danmu", self.value as i64)
    }
}
