#![doc = include_str!("../README.md")]

use std::{future::Future, num::NonZeroUsize, time::Duration};

use futures_util::{TryStreamExt, stream};
use serde::{Deserialize, Serialize};
pub use tokio_util::sync::CancellationToken;

/// The next action available from a checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Position<S> {
    Ready(S),
    Paused(S),
    Completed,
}

/// A workflow cursor and its cumulative number of committed steps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint<S> {
    position: Position<S>,
    completed_steps: usize,
}

impl<S> Checkpoint<S> {
    pub fn new(state: S) -> Self {
        Self {
            position: Position::Ready(state),
            completed_steps: 0,
        }
    }

    pub fn position(&self) -> &Position<S> {
        &self.position
    }

    pub fn completed_steps(&self) -> usize {
        self.completed_steps
    }

    /// Mark a paused checkpoint ready after the caller has accepted any required input.
    pub fn resume(mut self) -> Self {
        if let Position::Paused(state) = self.position {
            self.position = Position::Ready(state);
        }
        self
    }
}

/// Bounds for an execution segment.
#[derive(Clone, Copy, Debug)]
pub struct RunLimits {
    /// Total committed steps allowed, including steps before a pause.
    pub max_steps: NonZeroUsize,
    /// Time allowed for this invocation, including checkpoint writes.
    pub timeout: Duration,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    Paused,
    Completed,
}

#[derive(Debug, thiserror::Error)]
pub enum RunError<E, C> {
    #[error("workflow was cancelled")]
    Cancelled,
    #[error("workflow execution timed out")]
    TimedOut,
    #[error("workflow step limit reached")]
    StepLimitReached,
    #[error("workflow step failed: {0}")]
    Step(E),
    #[error("checkpoint commit failed: {0}")]
    Commit(C),
}

/// Run until completion, pause, or error. Each transition is committed before
/// the next step starts. A failed commit leaves the supplied cursor at its last
/// acknowledged position.
///
/// Callers own atomicity, deduplication and recovery for external effects. If a
/// commit is interrupted or its acknowledgement is lost, reload the checkpoint
/// from the store before retrying.
pub async fn run<S, E, C, Step, Work, Commit, Save>(
    checkpoint: &mut Checkpoint<S>,
    limits: RunLimits,
    cancel: &CancellationToken,
    mut step: Step,
    mut commit: Commit,
) -> Result<RunOutcome, RunError<E, C>>
where
    S: Clone,
    Step: FnMut(S) -> Work,
    Work: Future<Output = Result<Position<S>, E>>,
    Commit: FnMut(Checkpoint<S>) -> Save,
    Save: Future<Output = Result<(), C>>,
{
    let execution = async {
        loop {
            let state = match &checkpoint.position {
                Position::Ready(state) => state,
                Position::Paused(_) => return Ok(RunOutcome::Paused),
                Position::Completed => return Ok(RunOutcome::Completed),
            };
            if checkpoint.completed_steps >= limits.max_steps.get() {
                return Err(RunError::StepLimitReached);
            }
            // Both callbacks may return immediately, so check at every iteration.
            tokio::task::yield_now().await;
            if cancel.is_cancelled() {
                return Err(RunError::Cancelled);
            }
            let position = step(state.clone()).await.map_err(RunError::Step)?;
            if cancel.is_cancelled() {
                return Err(RunError::Cancelled);
            }
            let next = Checkpoint {
                position,
                completed_steps: checkpoint.completed_steps + 1,
            };
            commit(next.clone()).await.map_err(RunError::Commit)?;
            *checkpoint = next;
        }
    };

    tokio::select! {
        biased;
        _ = cancel.cancelled() => Err(RunError::Cancelled),
        result = tokio::time::timeout(limits.timeout, execution) => {
            result.unwrap_or(Err(RunError::TimedOut))
        }
    }
}

/// Process a batch with a fixed concurrency limit. Each callback can commit its
/// own result before the batch completes. Supply only pending items when
/// restoring a partially completed batch.
///
/// On error, the remaining in-flight futures are dropped. Committed results
/// remain the caller's responsibility to load on recovery.
pub async fn run_batch<I, Work, Job, E>(
    items: I,
    concurrency: NonZeroUsize,
    job: Work,
) -> Result<(), E>
where
    I: IntoIterator,
    Work: FnMut(I::Item) -> Job,
    Job: Future<Output = Result<(), E>>,
{
    stream::iter(items.into_iter().map(Ok))
        .try_for_each_concurrent(concurrency.get(), job)
        .await
}
