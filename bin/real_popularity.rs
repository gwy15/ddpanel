//! 导出真实人气值
use std::collections::{HashMap, VecDeque};

use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;

use crate::prelude::*;

#[derive(Debug, Serialize)]
struct Point {
    #[serde(with = "chrono::serde::ts_milliseconds")]
    time: DateTime<Utc>,
    popularity: u32,
}

/// 解析出 user, room_id, time
fn line_parse(s: &str) -> Option<(u64, u64, DateTime<Utc>)> {
    let line: RawLine = serde_json::from_str(s).unwrap();
    if line.operation != "SendMsgReply" {
        return None;
    }
    let time = line.time;
    let room_id = line.room_id;
    let body = line.body;
    let user_id = if body.contains("DANMU_MSG") {
        let danmu: DanmuMsg = serde_json::from_str(&body).unwrap_or_else(|e| {
            eprintln!("line = {}", s);
            eprintln!("body = {}", body);
            panic!("{:?}", e);
        });
        danmu.user_id
    } else if body.contains("SUPER_CHAT_MESSAGE\\") || body.contains("GUARD_BUY") {
        let v: Value = serde_json::from_str(&body).unwrap();
        let uid = v
            .get("data")
            .expect("data not found")
            .get("uid")
            .unwrap_or_else(|| {
                panic!("uid not found: {:?}", v);
            });
        match uid {
            Value::Number(i) => i.as_u64().unwrap(),
            Value::String(s) => s.parse().unwrap(),
            _ => panic!("unknown uid: {}", uid),
        }
    } else {
        return None;
    };

    Some((user_id, room_id, time))
}

pub async fn run(
    reader: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
    room_id: u64,
) -> Result<()> {
    let mut ans: Vec<Point> = vec![];

    // user_id, time
    let mut lines_buffer: VecDeque<(u64, DateTime<Utc>)> = VecDeque::new();
    // user_id => times
    let mut user_count: HashMap<u64, u32> = HashMap::new();

    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        // 统计弹幕、舰长和superchat
        let (user_id, _room_id, time) = match line_parse(&line) {
            None => continue,
            Some((_, _room_id, _)) if _room_id != room_id => continue,
            Some((a, b, c)) => (a, b, c),
        };
        lines_buffer.push_back((user_id, time));
        *user_count.entry(user_id).or_default() += 1;
        // 五分钟滑动窗口
        let min = time - chrono::Duration::minutes(5);
        while lines_buffer.front().map(|t| t.1 < min).unwrap_or(false) {
            let (user_id, _) = lines_buffer.pop_front().unwrap();
            match user_count.get_mut(&user_id) {
                Some(t) if *t == 1 => {
                    user_count.remove(&user_id);
                }
                Some(t) => {
                    *t -= 1;
                }
                None => {}
            }
        }
        if let Some(pt) = ans.last() {
            // 最大分辨率五秒
            if pt.time > time - chrono::Duration::seconds(5) {
                continue;
            }
        }
        ans.push(Point {
            time,
            popularity: user_count.len() as u32,
        });
    }

    info!("{} points", ans.len());
    let s = serde_json::to_string(&ans)?;
    output.write_all(s.as_bytes()).await?;
    output.flush().await?;
    output.shutdown().await?;

    Ok(())
}
