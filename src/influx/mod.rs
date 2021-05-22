mod messages;

mod influx_appender;
pub use influx_appender::InfluxAppender;

/// influx 最好有额外的信息（房间名）
pub struct RoomInfo {
    pub id: u64,
    pub streamer: String,
}
impl RoomInfo {
    #[inline(always)]
    pub fn from_cache_opt(id: u64) -> Option<Self> {
        crate::manager::ROOM_ID_TO_STREAMER
            .read()
            .get(&id)
            .cloned()
            .map(|streamer| RoomInfo { id, streamer })
    }

    #[inline(always)]
    pub fn from_cache(id: u64) -> Self {
        Self::from_cache_opt(id).unwrap_or_else(|| {
            warn!("room {} streamer name not found!", id);
            Self {
                id,
                streamer: id.to_string(),
            }
        })
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
