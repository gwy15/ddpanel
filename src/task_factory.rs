use std::{collections::HashSet, num::ParseIntError, path::PathBuf, time::Duration};

use anyhow::{Context, Result};
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
            match self.load_tasks().await {
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
                    warn!(
                        "task factory failed to load tasks: {:?}. Will try again.",
                        e
                    );
                }
            }
            tokio::time::sleep(REFRESH_DURATION).await;
        }
    }

    async fn load_tasks(&self) -> Result<TaskSet> {
        let content = tokio::fs::read_to_string(&self.task_file).await?;
        Self::parse_content(&content).context("Failed to parse task file")
    }

    fn parse_content(content: &str) -> Result<TaskSet> {
        let mut tasks = TaskSet::new();
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            if line.starts_with('#') || line.starts_with("//") {
                continue;
            }
            let line: TaskSet = line
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.parse())
                .collect::<Result<TaskSet, ParseIntError>>()?;
            tasks.extend(line);
        }
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
