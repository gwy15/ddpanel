mod messages;

mod influx_appender;
pub use influx_appender::InfluxAppender;

/// influx 最好有额外的信息（房间名）
pub struct RoomInfo {
    pub id: u64,
    pub streamer: String,
}
impl RoomInfo {
    pub fn from_cache(id: u64) -> Self {
        let streamer = crate::manager::ROOM_ID_TO_STREAMER
            .read()
            .get(&id)
            .cloned()
            .unwrap_or_else(|| {
                warn!("room {} streamer name not found!", id);
                id.to_string()
            });
        let room_info = RoomInfo { id, streamer };
        room_info
    }
    pub fn write_cache(id: u64, streamer: String) {
        crate::manager::ROOM_ID_TO_STREAMER
            .write()
            .insert(id, streamer.clone());
        info!(
            "wrote streamer name = {} to cache (room = {})",
            streamer, id
        );
    }
}
