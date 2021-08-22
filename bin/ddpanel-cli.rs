//! 导出弹幕文件
#[macro_use]
extern crate log;

use anyhow::*;
use chrono::{DateTime, Utc};
use clap::Clap;
use serde::Deserialize;
use tokio::{
    fs::File,
    io::{AsyncBufRead, BufReader, BufWriter},
};

mod danmu;
mod export_danmu;
mod real_popularity;
mod prelude {
    pub use crate::danmu::DanmuMsg;
    pub use crate::RawLine;
    pub use anyhow::*;
    pub use tokio::{
        fs::File,
        io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader, BufWriter},
    };
}

#[derive(Debug, clap::Clap)]
enum Action {
    #[clap(about = "导出弹幕")]
    ExportDanmu,
    #[clap(about = "五分钟同接")]
    Popularity,
}

#[derive(Debug, clap::Clap)]
struct Options {
    #[clap(long = "input", short = 'i', about = "input file")]
    input: String,

    #[clap(
        long = "output",
        short = 'o',
        about = "output",
        default_value = "output.json"
    )]
    output: String,

    #[clap(long = "room", short = 'r', about = "room_id")]
    room: u64,

    #[clap(subcommand)]
    action: Action,
}

#[derive(Debug, Deserialize)]
pub struct RawLine {
    operation: String,
    body: String,
    time: DateTime<Utc>,
    room_id: u64,
}

#[tokio::main]
async fn main() -> Result<()> {
    if log4rs::init_file("log4rs.yml", Default::default()).is_err() {
        pretty_env_logger::init_timed();
    }

    let args = Options::parse();

    info!("replaying file {:?}", args.input);
    let f = File::open(&args.input).await?;
    let f = BufReader::new(f);

    let writer = File::create(args.output).await?;
    let writer = BufWriter::new(writer);

    let reader: Box<dyn AsyncBufRead + Unpin> = if args.input.ends_with("gz") {
        info!("gz detected, treating as gz");
        let reader = async_compression::tokio::bufread::GzipDecoder::new(f);
        Box::new(BufReader::new(reader))
    } else {
        Box::new(f)
    };
    match args.action {
        Action::ExportDanmu => {
            export_danmu::run(reader, writer, args.room).await?;
        }
        Action::Popularity => {
            real_popularity::run(reader, writer, args.room).await?;
        }
    }

    Ok(())
}
