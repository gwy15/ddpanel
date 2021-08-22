use crate::prelude::*;

pub async fn run(
    reader: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
    room_id: u64,
) -> Result<()> {
    let mut messages = vec![];

    let mut lines = reader.lines();
    while let Some(line) = lines.next_line().await? {
        if !(line.contains("SendMsgReply") && line.contains("DANMU_MSG")) {
            continue;
        }
        let line: RawLine = serde_json::from_str(&line)?;
        if !(line.room_id == room_id && line.operation == "SendMsgReply") {
            continue;
        }
        let danmu_msg: DanmuMsg = serde_json::from_str(&line.body)?;
        messages.push(danmu_msg);
    }

    info!("{} 弹幕", messages.len());

    let s = serde_json::to_string(&messages)?;
    output.write_all(s.as_bytes()).await?;
    output.flush().await?;
    output.shutdown().await?;

    Ok(())
}
