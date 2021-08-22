use anyhow::*;
use chrono::{DateTime, NaiveDateTime, Utc};
use serde::{de::Error as SerdeError, Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Serialize)]
pub struct DanmuMsg {
    pub text: String,
    pub user_id: u64,
    pub username: String,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    pub time: DateTime<Utc>,
}

impl<'de> Deserialize<'de> for DanmuMsg {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        type V = Value;
        #[allow(clippy::type_complexity)]
        #[derive(Deserialize)]
        struct Body {
            cmd: String,
            info: (
                // 4. timestamp
                (V, V, V, V, u64, V, V, V, V, V, V, V, V, V, Option<V>), // 0.
                String,                                                  // 1. msg
                // uid, uname,
                (u64, String, u64, u64, u64, u64, u64, String), // 2.
                V,
                V,
                V,
                V,
                V,
                V, // 3 - 8
                V,
                V,
                V,
                V,
                V,
                V, // 9 - 14
                V, // 15
            ),
        }
        let body = Body::deserialize(deserializer)?;
        if body.cmd != "DANMU_MSG" {
            return Err(D::Error::custom("not danmu msg"));
        }

        let ts = body.info.0 .4;
        let t = NaiveDateTime::from_timestamp(ts as i64 / 1_000, (ts % 1000) as u32 * 1_000_000);

        Ok(DanmuMsg {
            text: body.info.1,
            user_id: body.info.2 .0,
            username: body.info.2 .1,
            time: DateTime::from_utc(t, Utc),
        })
    }
}
