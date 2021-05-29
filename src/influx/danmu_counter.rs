use std::{
    collections::HashMap,
    time::{Duration, Instant},
};

use chrono::{DateTime, FixedOffset, Local, NaiveDateTime};
use influxdb_client::Point;

use super::{
    messages::{Danmu, ToPoint},
    RoomInfo,
};

const DANMU_INTERVAL: Duration = Duration::from_secs(1);

pub struct DanmuCounter {
    ///
    last_flush: Instant,

    /// 房间的弹幕
    danmu_counter: HashMap<i64, HashMap<u64, u32>>,
}

impl DanmuCounter {
    pub fn new() -> Self {
        Self {
            last_flush: Instant::now(),
            danmu_counter: HashMap::new(),
        }
    }

    pub fn count(&mut self, room_id: u64, t: DateTime<Local>) {
        let t = t.timestamp();
        *self
            .danmu_counter
            .entry(t)
            .or_default()
            .entry(room_id)
            .or_default() += 1;
    }

    pub fn flush(&mut self) -> Vec<Point> {
        if Instant::now().duration_since(self.last_flush) > DANMU_INTERVAL {
            self.last_flush = Instant::now();
        } else {
            return vec![];
        }
        let swap = std::mem::take(&mut self.danmu_counter);
        let mut points = vec![];
        let offset = FixedOffset::east(8 * 3600);
        for (secs, rooms) in swap.into_iter() {
            let t = NaiveDateTime::from_timestamp(secs, 0);
            let t = DateTime::<Local>::from_utc(t, offset);
            for (room_id, count) in rooms {
                let room_info = RoomInfo::from_cache(room_id);
                debug!(
                    "{} danmu in {} @ {}",
                    count,
                    t.format("%H:%M:%S"),
                    room_info.streamer
                );
                let p = Danmu::new(count).into_point(&room_info, t);
                points.push(p);
            }
        }
        points
    }
}
