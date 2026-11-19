use std::{
    convert::Infallible,
    fs::{self, OpenOptions},
    io,
    num::NonZeroUsize,
    path::PathBuf,
    process::{self, Command},
    time::{SystemTime, UNIX_EPOCH},
};

use pecs_graph::{Store, StoreError, Stored, TaskKey, run_stored_batch};

// The restart fixture only needs insert-once storage for one batch of integers.
struct FileResults(PathBuf);

impl Store<TaskKey, u32> for FileResults {
    type Error = io::Error;

    async fn load(&self, key: &TaskKey) -> Result<Option<Stored<u32>>, io::Error> {
        match fs::read(self.0.join(&key.task_id)) {
            Ok(bytes) => Ok(Some(Stored {
                revision: 1,
                value: serde_json::from_slice(&bytes)?,
            })),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn compare_and_put(
        &self,
        key: &TaskKey,
        expected_revision: Option<u64>,
        output: &u32,
    ) -> Result<u64, StoreError<io::Error>> {
        assert_eq!(expected_revision, None);
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(self.0.join(&key.task_id))
            .map_err(|error| match error.kind() {
                io::ErrorKind::AlreadyExists => StoreError::Conflict,
                _ => StoreError::Backend(error),
            })?;
        serde_json::to_writer(&file, output).map_err(|error| StoreError::Backend(error.into()))?;
        file.sync_all().map_err(StoreError::Backend)?;
        Ok(1)
    }
}

#[tokio::test]
async fn resumes_a_partially_committed_batch_in_another_process() {
    const DIRECTORY: &str = "PECS_GRAPH_RESTART_TEST_DIRECTORY";
    const PHASE: &str = "PECS_GRAPH_RESTART_TEST_PHASE";

    if let Some(directory) = std::env::var_os(DIRECTORY) {
        let phase = std::env::var(PHASE).unwrap();
        let outputs = run_stored_batch(
            &FileResults(PathBuf::from(directory)),
            "run",
            "batch",
            [1, 2, 3].map(|id| (id.to_string(), id)),
            NonZeroUsize::MIN,
            |id| {
                let phase = &phase;
                async move {
                    if phase == "first" && id == 3 {
                        // Stop the entire process before the third output is saved.
                        process::exit(42);
                    }
                    if phase == "resume" {
                        assert_eq!(id, 3);
                    }
                    Ok::<_, Infallible>(id * 10)
                }
            },
        )
        .await
        .unwrap();
        assert_eq!(phase, "resume");
        assert_eq!(outputs, [10, 20, 30]);
        return;
    }

    let directory = std::env::temp_dir().join(format!(
        "pecs-graph-restart-{}-{}",
        process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    fs::create_dir(&directory).unwrap();
    let run = |phase| {
        Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "resumes_a_partially_committed_batch_in_another_process",
            ])
            .env(DIRECTORY, &directory)
            .env(PHASE, phase)
            .output()
            .unwrap()
    };
    let first = run("first");
    let resumed = run("resume");
    fs::remove_dir_all(directory).unwrap();
    assert_eq!(first.status.code(), Some(42));
    assert!(
        resumed.status.success(),
        "{}",
        String::from_utf8_lossy(&resumed.stdout)
    );
}
