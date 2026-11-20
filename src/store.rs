use std::{
    collections::{HashMap, HashSet},
    future::Future,
    hash::Hash,
    num::NonZeroUsize,
    sync::Mutex,
};

use futures_util::{StreamExt, TryStreamExt, stream};

use crate::{
    CancellationToken, Checkpoint, CheckpointWriter, Position, RunError, RunLimits, RunOutcome,
    Workflow,
};

/// A stored value and the revision to use for its next conditional write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Stored<V> {
    /// Current revision for the next conditional write.
    pub revision: u64,
    /// Value read at that revision.
    pub value: V,
}

/// Failure from a conditional store write.
#[derive(Debug, thiserror::Error)]
pub enum StoreError<E> {
    /// The value at the key did not match the expected revision or absence.
    #[error("stored revision has changed")]
    Conflict,
    /// The backend could not acknowledge the write.
    #[error("store operation failed: {0}")]
    Backend(E),
}

/// Storage for checkpoints or task results.
pub trait Store<K, V>: Send + Sync {
    /// Backend error reported while reading or writing.
    type Error;

    /// Load the value and revision at `key`, or return `None` for an absent key.
    fn load(&self, key: &K) -> impl Future<Output = Result<Option<Stored<V>>, Self::Error>> + Send;

    /// Atomically save if the current revision matches. Use `None` to create a
    /// record only if the key is absent. Return the new revision on success.
    /// If the future is dropped or the backend cannot acknowledge the write,
    /// the value may already have been stored. Reload before retrying.
    fn compare_and_put(
        &self,
        key: &K,
        expected_revision: Option<u64>,
        value: &V,
    ) -> impl Future<Output = Result<u64, StoreError<Self::Error>>> + Send;
}

/// Process-local storage for development, tests and in-process resume.
/// Records are retained for the lifetime of the store.
pub struct MemoryStore<K, V> {
    entries: Mutex<HashMap<K, Stored<V>>>,
}

impl<K, V> Default for MemoryStore<K, V> {
    fn default() -> Self {
        Self {
            entries: Mutex::new(HashMap::new()),
        }
    }
}

/// Failure from the in-memory store.
#[derive(Debug, thiserror::Error)]
pub enum MemoryStoreError {
    /// A panic poisoned the store lock.
    #[error("memory store lock was poisoned")]
    Poisoned,
    /// The next revision would exceed the range of `u64`.
    #[error("stored revision exhausted")]
    RevisionExhausted,
}

impl<K, V> Store<K, V> for MemoryStore<K, V>
where
    K: Clone + Eq + Hash + Send + Sync,
    V: Clone + Send + Sync,
{
    type Error = MemoryStoreError;

    async fn load(&self, key: &K) -> Result<Option<Stored<V>>, Self::Error> {
        let entries = self
            .entries
            .lock()
            .map_err(|_| MemoryStoreError::Poisoned)?;
        Ok(entries.get(key).cloned())
    }

    async fn compare_and_put(
        &self,
        key: &K,
        expected_revision: Option<u64>,
        value: &V,
    ) -> Result<u64, StoreError<Self::Error>> {
        let mut entries = self
            .entries
            .lock()
            .map_err(|_| StoreError::Backend(MemoryStoreError::Poisoned))?;
        if entries.get(key).map(|entry| entry.revision) != expected_revision {
            return Err(StoreError::Conflict);
        }
        let revision = expected_revision
            .unwrap_or(0)
            .checked_add(1)
            .ok_or(StoreError::Backend(MemoryStoreError::RevisionExhausted))?;
        entries.insert(
            key.clone(),
            Stored {
                revision,
                value: value.clone(),
            },
        );
        Ok(revision)
    }
}

/// Failure to load or advance a stored workflow.
#[derive(Debug, thiserror::Error)]
pub enum StoredRunError<E, C> {
    /// The run key has no checkpoint.
    #[error("checkpoint does not exist")]
    Missing,
    /// Loading the checkpoint failed.
    #[error("could not load checkpoint: {0}")]
    Load(C),
    /// The executor could not advance the checkpoint.
    #[error(transparent)]
    Run(#[from] RunError<E, StoreError<C>>),
}

/// Run an existing checkpoint under a stable run key and commit each step
/// against its current revision.
///
/// Conditional commits reject stale writes. Applications that schedule a run
/// on multiple workers must coordinate ownership to prevent duplicate work.
///
/// `store` must already contain the initial checkpoint at `key`.
/// `limits.timeout` covers the initial read, step execution, and commits.
/// `cancel` and `step` have the same semantics as in [`crate::run`].
/// Each invocation reloads the stored checkpoint. If a write could not be
/// acknowledged, inspect the stored state before invoking external effects again.
/// A paused checkpoint returns its current outcome until you save a resumed
/// checkpoint against its current revision.
pub async fn run_stored<K, S, E, D, Work>(
    store: &D,
    key: &K,
    limits: RunLimits,
    cancel: &CancellationToken,
    step: impl FnMut(S) -> Work,
) -> Result<RunOutcome, StoredRunError<E, D::Error>>
where
    S: Clone,
    D: Store<K, Checkpoint<S>>,
    Work: Future<Output = Result<Position<S>, E>>,
{
    let started = tokio::time::Instant::now();
    let loaded = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(RunError::Cancelled.into()),
        result = tokio::time::timeout(limits.timeout, store.load(key)) => {
            result.map_err(|_| RunError::TimedOut)?
        }
    };
    if cancel.is_cancelled() {
        return Err(RunError::Cancelled.into());
    }
    let elapsed = started.elapsed();
    if elapsed >= limits.timeout {
        return Err(RunError::TimedOut.into());
    }
    let Stored {
        mut value,
        revision,
    } = loaded
        .map_err(StoredRunError::Load)?
        .ok_or(StoredRunError::Missing)?;
    let outcome = crate::run_with_checkpoints(
        &mut value,
        RunLimits {
            timeout: limits.timeout.saturating_sub(elapsed),
            ..limits
        },
        cancel,
        step,
        StoreWriter {
            store,
            key,
            revision,
        },
    )
    .await?;
    Ok(outcome)
}

/// Run an existing stored checkpoint using a [`Workflow`] implementation.
///
/// Checkpoint persistence is handled by `store` via [`run_stored`]; [`Workflow::commit`]
/// is not invoked.
pub async fn run_stored_workflow<K, S, W, D>(
    store: &D,
    key: &K,
    limits: RunLimits,
    cancel: &CancellationToken,
    workflow: &W,
) -> Result<RunOutcome, StoredRunError<W::Error, D::Error>>
where
    S: Clone,
    W: Workflow<S>,
    D: Store<K, Checkpoint<S>>,
{
    run_stored(store, key, limits, cancel, |state| workflow.step(state)).await
}

struct StoreWriter<'a, D, K> {
    store: &'a D,
    key: &'a K,
    revision: u64,
}

impl<D, K, S> CheckpointWriter<S> for StoreWriter<'_, D, K>
where
    D: Store<K, Checkpoint<S>>,
{
    type Error = StoreError<D::Error>;

    async fn write(&mut self, checkpoint: &Checkpoint<S>) -> Result<(), Self::Error> {
        self.revision = self
            .store
            .compare_and_put(self.key, Some(self.revision), checkpoint)
            .await?;
        Ok(())
    }
}

/// Identity of one item in one batch invocation.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TaskKey {
    /// Stable identity of the enclosing workflow.
    pub run_id: String,
    /// Identity of this batch invocation within the workflow.
    pub batch_id: String,
    /// Unique identity of the item within the batch.
    pub task_id: String,
}

/// Failure while looking up, executing, or saving a batch item.
#[derive(Debug, thiserror::Error)]
pub enum BatchError<E, C> {
    /// An input ID occurs more than once in the batch.
    #[error("duplicate batch task ID: {0}")]
    DuplicateTask(String),
    /// A job returned a failure.
    #[error("task {task_id} failed: {error}")]
    Task {
        /// ID of the failed item.
        task_id: String,
        /// Error returned by its job.
        error: E,
    },
    /// A saved result could not be loaded.
    #[error("could not load task {task_id}: {error}")]
    Load {
        /// ID of the item being loaded.
        task_id: String,
        /// Error returned by the store.
        error: C,
    },
    /// A new result could not be acknowledged.
    #[error("could not save task {task_id}: {error}")]
    Save {
        /// ID of the item being saved.
        task_id: String,
        /// Store error or revision conflict.
        error: StoreError<C>,
    },
}

/// Run a finite batch and commit each output independently.
///
/// Results are returned in input order. Reuse the same IDs and inputs for a
/// retry. Use a new batch ID for a new invocation or different inputs.
/// A task without a saved result can run again after interruption or on a
/// concurrent worker. Use idempotent task effects or coordinate execution.
///
/// `store` reads and writes results keyed by `run_id`, `batch_id`, and the item
/// ID from `items`. The finite input is collected and task IDs are validated
/// before any job starts. `concurrency` bounds active item futures, including
/// storage I/O. `job` receives the payload of an item whose result is absent.
///
/// On error, in-flight futures are dropped. A previously committed result can
/// be read on retry. A conflicting result write is reported as a save error.
pub async fn run_stored_batch<I, O, E, Work, Job, D>(
    store: &D,
    run_id: &str,
    batch_id: &str,
    items: impl IntoIterator<Item = (String, I)>,
    concurrency: NonZeroUsize,
    job: Work,
) -> Result<Vec<O>, BatchError<E, D::Error>>
where
    D: Store<TaskKey, O>,
    Work: Fn(I) -> Job,
    Job: Future<Output = Result<O, E>>,
{
    let mut batch = Vec::new();
    for item in items {
        tokio::task::consume_budget().await;
        batch.push(item);
    }
    let mut ids = HashSet::new();
    for (id, _) in &batch {
        tokio::task::consume_budget().await;
        if !ids.insert(id) {
            return Err(BatchError::DuplicateTask(id.clone()));
        }
    }
    let job = &job;
    let mut results: Vec<_> = stream::iter(batch.into_iter().enumerate())
        .then(|item| async {
            tokio::task::consume_budget().await;
            item
        })
        .map(|(index, (task_id, input))| async move {
            let key = TaskKey {
                run_id: run_id.to_owned(),
                batch_id: batch_id.to_owned(),
                task_id,
            };
            if let Some(stored) = store.load(&key).await.map_err(|error| BatchError::Load {
                task_id: key.task_id.clone(),
                error,
            })? {
                return Ok((index, stored.value));
            }
            let output = job(input).await.map_err(|error| BatchError::Task {
                task_id: key.task_id.clone(),
                error,
            })?;
            store
                .compare_and_put(&key, None, &output)
                .await
                .map_err(|error| BatchError::Save {
                    task_id: key.task_id,
                    error,
                })?;
            Ok((index, output))
        })
        .buffer_unordered(concurrency.get())
        .try_collect()
        .await?;
    results.sort_unstable_by_key(|(index, _)| *index);
    Ok(results.into_iter().map(|(_, output)| output).collect())
}
