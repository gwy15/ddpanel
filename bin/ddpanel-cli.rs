//! 导出弹幕文件
use anyhow::*;
use chrono::{DateTime, Utc};
use clap::Clap;
use log::*;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};

mod danmu;
use danmu::DanmuMsg;

#[derive(Debug, clap::Clap)]
struct Opts {
    #[clap(long = "file", short = 'f', about = "file")]
    file: String,

    #[clap(long = "output", short = 'o', about = "output")]
    output: String,

    #[clap(long = "room", short = 'r', about = "room_id")]
    room: u64,
}

#[derive(Debug, Deserialize)]
struct RawLine {
    operation: String,
    body: String,
    time: DateTime<Utc>,
    room_id: u64,
}

async fn run(
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

#[tokio::main]
async fn main() -> Result<()> {
    if log4rs::init_file("log4rs.yml", Default::default()).is_err() {
        pretty_env_logger::init_timed();
    }

    let opts = Opts::parse();

    info!("replaying file {:?}", opts.file);
    let f = File::open(&opts.file).await?;
    let f = BufReader::new(f);

    let writer = File::create(opts.output).await?;
    let writer = BufWriter::new(writer);

    let reader: Box<dyn AsyncBufRead + Unpin> = if opts.file.ends_with("gz") {
        info!("gz detected, treating as gz");
        let reader = async_compression::tokio::bufread::GzipDecoder::new(f);
        Box::new(BufReader::new(reader))
    } else {
        Box::new(f)
    };
    run(reader, writer, opts.room).await?;

    Ok(())
}
