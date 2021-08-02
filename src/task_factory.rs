use serde::Deserialize;
use std::{collections::HashSet, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
use tokio::sync::mpsc;

const REFRESH_DURATION: Duration = Duration::from_secs(10);

pub type TaskSet = HashSet<u64>;

#[derive(Debug, Deserialize, Clone, PartialEq, Eq)]
pub struct Config {
    pub live_rooms: TaskSet,
    pub users: TaskSet,
}

pub struct TaskFactory {
    task_file: PathBuf,
    sender: mpsc::Sender<Config>,
    last: Option<Config>,
}

impl TaskFactory {
    pub fn start(task_file: PathBuf) -> mpsc::Receiver<Config> {
        let (sender, receiver) = mpsc::channel(5);
        let factory = Self {
            task_file,
            sender,
            last: None,
        };
        tokio::spawn(factory.run());
        receiver
    }

    /// 一直循环解析任务，或等到特定的信号结束
    async fn run(mut self) -> Result<()> {
        loop {
            match self.load_tasks().await {
                Ok(tasks) => match self.last.as_ref() {
                    None => {
                        info!("task initialized => tasks = {:?}", tasks);
                        self.last = Some(tasks.clone());
                        self.sender.send(tasks).await?;
                    }
                    Some(last) if last != &tasks => {
                        info!("task changed: {:?} => {:?}", last, tasks);
                        self.last = Some(tasks.clone());
                        self.sender.send(tasks).await?;
                    }
                    Some(_last) => {
                        debug!("task not changed.");
                    }
                },
                Err(e) => {
                    warn!(
                        "task factory failed to load tasks: {:?}. Will try again.",
                        e
                    );
                }
            }
            tokio::select! {
                _ = Self::stop_signal() => {
                    std::mem::drop(self.sender);
                    info!("ctrl+c received, stopping task_factory.");
                    return Ok(());
                }
                _ = tokio::time::sleep(REFRESH_DURATION) => {}
            }
        }
    }

    #[cfg(not(unix))]
    async fn stop_signal() {
        let _ = tokio::signal::ctrl_c().await.unwrap();
    }

    #[cfg(unix)]
    async fn stop_signal() {
        use tokio::signal;
        let mut sigterm = signal::unix::signal(signal::unix::SignalKind::terminate()).unwrap();

        tokio::select! {
            _ = signal::ctrl_c() => {
                info!("ctrl-c received. quitting.");
            },
            _ = sigterm.recv() => {
                info!("SIGTERM recived. quitting.");
            }
        }
    }

    async fn load_tasks(&self) -> Result<Config> {
        let content = tokio::fs::read_to_string(&self.task_file).await?;
        Self::parse_content(&content).context("Failed to parse task file")
    }

    fn parse_content(content: &str) -> Result<Config> {
        let tasks = toml::from_str(content)?;
        // for line in content.lines() {
        //     let line = line.trim();
        //     if line.is_empty() {
        //         continue;
        //     }
        //     if line.starts_with('#') || line.starts_with("//") {
        //         continue;
        //     }
        //     let line: TaskSet = line
        //         .split(',')
        //         .map(|s| s.trim())
        //         .filter(|s| !s.is_empty())
        //         .map(|s| s.parse())
        //         .collect::<Result<TaskSet, ParseIntError>>()?;
        //     tasks.extend(line);
        // }
        Ok(tasks)
    }
}

#[test]
fn test_parse_content() {
    macro_rules! set {
        ($($i:expr),*) => {{
            let mut s = TaskSet::default();
            $(
                s.insert($i);
            )*
            s
        }}
    }
    assert_eq!(
        TaskFactory::parse_content(
            r#"
            # test
            // test
            1
            # 
            2
            #
            2
            #
            1
        "#
        )
        .unwrap(),
        set!(1, 2)
    );

    assert_eq!(
        TaskFactory::parse_content(
            r#"
            # test
            // test
            1,2,3
            # 
            2,3,4,5
            #
            2, 5,       3,
            #
            1
        "#
        )
        .unwrap(),
        set!(1, 2, 3, 4, 5)
    );

    assert!(TaskFactory::parse_content(
        r#"
            # test
            // test
            1,2,3
            # 
            2,3,4,5,a
            #
            2, 5,       3,
            #
            1
        "#
    )
    .is_err())
}
