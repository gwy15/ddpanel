use anyhow::*;
use chrono::{DateTime, NaiveDateTime, Utc};
use clap::Clap;
use log::*;
use serde::{de::Error as SerdeError, Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    fs::File,
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
};

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
struct Line {
    operation: String,
    body: String,
    time: DateTime<Utc>,
    room_id: u64,
}

#[derive(Debug, Serialize)]
struct DanmuMsg {
    text: String,
    user_id: u64,
    username: String,
    #[serde(with = "chrono::serde::ts_milliseconds")]
    time: DateTime<Utc>,
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
                (V, V, V, V, u64, V, V, V, V, V, V, V, V, V), // 0.
                String,                                       // 1. msg
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
        let line: Line = serde_json::from_str(&line)?;
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
    log4rs::init_file("log4rs.yml", Default::default())?;

    let opts = Opts::parse();

    info!("replaying file {:?}", opts.file);
    let f = File::open(&opts.file).await?;
    let f = BufReader::new(f);

    let writer = File::create(opts.output).await?;
    let writer = BufWriter::new(writer);

    if opts.file.ends_with("gz") {
        info!("gz detected, treating as gz");
        let reader = async_compression::tokio::bufread::GzipDecoder::new(f);
        let reader = BufReader::new(reader);
        run(reader, writer, opts.room).await?;
    } else {
        run(f, writer, opts.room).await?;
    }

    Ok(())
}
