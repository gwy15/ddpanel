use influxdb_client::Point;

#[derive(Debug, Deserialize)]
pub struct Popularity {
    value: i64,
}
impl Popularity {
    pub fn new(value: i64) -> Self {
        Self { value }
    }
}
impl super::ToPoint for Popularity {
    fn into_basic_point(self) -> Point {
        Point::new("live-popularity").field("popularity", self.value)
    }
}
