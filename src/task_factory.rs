use std::{collections::HashSet, path::PathBuf, time::Duration};

use anyhow::Result;
use tokio::sync::mpsc;

const REFRESH_DURATION: Duration = Duration::from_secs(10);

pub type TaskSet = HashSet<u64>;

pub struct TaskFactory {
    task_file: PathBuf,
    sender: mpsc::Sender<TaskSet>,
    last: Option<TaskSet>,
}

impl TaskFactory {
    pub fn start(task_file: PathBuf) -> mpsc::Receiver<TaskSet> {
        let (sender, receiver) = mpsc::channel(5);
        let factory = Self {
            task_file,
            sender,
            last: None,
        };
        tokio::spawn(factory.run());
        receiver
    }

    async fn run(mut self) -> Result<()> {
        loop {
            match self.load().await {
                Ok(tasks) => match self.last.as_ref() {
                    None => {
                        info!("task initialized => {:?}", tasks);
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
                    warn!("task factory failed to load tasks: {:?}. Retrying...", e);
                }
            }
            tokio::time::sleep(REFRESH_DURATION).await;
        }
    }

    async fn load(&self) -> Result<TaskSet> {
        let mut tasks = TaskSet::default();
        let content = tokio::fs::read_to_string(&self.task_file).await?;
        for line in content.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if line.starts_with('#') || line.starts_with("//") {
                continue;
            }
            match line.parse::<u64>() {
                Ok(id) => {
                    tasks.insert(id);
                }
                Err(e) => {
                    warn!("Failed to parse task file: {:?}", e);
                }
            }
        }
        Ok(tasks)
    }
}
