#![doc = include_str!("../README.md")]

use std::{convert::Infallible, future::Future, num::NonZeroUsize, time::Duration};

use futures_util::{FutureExt, StreamExt, TryStreamExt, stream};
use serde::{Deserialize, Serialize};
pub use tokio_util::sync::CancellationToken;

mod store;
pub use store::{
    BatchError, MemoryStore, MemoryStoreError, Store, StoreError, Stored, StoredRunError, TaskKey,
    run_stored, run_stored_batch, run_stored_workflow,
};

/// The next action available from a checkpoint.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Position<S> {
    /// State to pass to the next step.
    Ready(S),
    /// Continuation to pass to the next step after an explicit resume.
    Paused(S),
    /// The workflow has reached its terminal position.
    Completed,
}

/// A workflow cursor and its cumulative number of committed steps.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Checkpoint<S> {
    position: Position<S>,
    completed_steps: usize,
}

impl<S> Checkpoint<S> {
    /// Create a ready checkpoint with its step counter set to zero.
    pub fn new(state: S) -> Self {
        Self {
            position: Position::Ready(state),
            completed_steps: 0,
        }
    }

    /// Return the current position and its continuation, when present.
    pub fn position(&self) -> &Position<S> {
        &self.position
    }

    /// Count acknowledged transitions, including those that pause or complete.
    pub fn completed_steps(&self) -> usize {
        self.completed_steps
    }

    /// Mark a paused checkpoint ready. A ready or completed checkpoint stays as it is.
    pub fn resume(self) -> Self {
        self.resume_with(std::convert::identity)
    }

    /// Apply accepted input to a paused continuation while retaining its step count.
    /// The callback runs only when the checkpoint is paused.
    pub fn resume_with(mut self, update: impl FnOnce(S) -> S) -> Self {
        if let Position::Paused(state) = self.position {
            self.position = Position::Ready(update(state));
        }
        self
    }
}

/// Bounds for an execution segment.
#[derive(Clone, Copy, Debug)]
pub struct RunLimits {
    /// Total committed transitions allowed, including earlier execution segments.
    /// A step can perform any number of application operations, such as model calls.
    pub max_steps: NonZeroUsize,
    /// Time allowed for this invocation, including checkpoint writes. A resumed
    /// invocation receives its own timeout. Futures must yield for interruption.
    pub timeout: Duration,
}

impl Default for RunLimits {
    /// Allow up to 128 committed steps and 30 seconds per execution segment.
    fn default() -> Self {
        Self {
            max_steps: NonZeroUsize::new(128).unwrap(),
            timeout: Duration::from_secs(30),
        }
    }
}

/// A position at which the executor successfully stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RunOutcome {
    /// The checkpoint holds a continuation waiting for explicit resume.
    Paused,
    /// The checkpoint is terminal.
    Completed,
}

/// The next step or final result of an ephemeral workflow.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Step<S, O> {
    /// Continue with this state.
    Continue(S),
    /// Finish and return this output to the caller.
    Complete(O),
}

/// Run an in-process workflow from `S::default()` with [`RunLimits::default()`].
///
/// `step` receives the current state and returns [`Step::Continue`] with the next
/// state or [`Step::Complete`] with the typed output. The default limits allow
/// 128 steps and 30 seconds. Use [`execute_with_limits`] to supply your own
/// initial state and limits.
///
/// # Errors
///
/// Returns a [`RunError`] if the step fails or execution exceeds its bounds.
pub async fn execute<S, O, E, Work>(
    step: impl FnMut(S) -> Work,
) -> Result<O, RunError<E, Infallible>>
where
    S: Clone + Default,
    Work: Future<Output = Result<Step<S, O>, E>>,
{
    execute_with_limits(S::default(), RunLimits::default(), step).await
}

/// Run an in-process workflow with a custom initial state and execution limits.
///
/// # Parameters
///
/// - `initial` is the state passed to the first step. It can be any cloneable value.
/// - `limits` sets the total step count and timeout for this execution. Use
///   [`RunLimits::default()`] for any fields you want to leave at their defaults.
/// - `step` receives the current state and returns [`Step::Continue`] with the
///   next state or [`Step::Complete`] with the final output.
///
/// # Errors
///
/// Returns a [`RunError`] if the step fails or execution exceeds its bounds.
pub async fn execute_with_limits<S, O, E, Work>(
    initial: S,
    limits: RunLimits,
    mut step: impl FnMut(S) -> Work,
) -> Result<O, RunError<E, Infallible>>
where
    S: Clone,
    Work: Future<Output = Result<Step<S, O>, E>>,
{
    let (_, output) = run_inner(
        &mut Checkpoint::new(initial),
        limits,
        &CancellationToken::new(),
        |state| {
            step(state).map(|result| {
                result.map(|step| match step {
                    Step::Continue(next) => (Position::Ready(next), None),
                    Step::Complete(output) => (Position::Completed, Some(output)),
                })
            })
        },
        DiscardCheckpoint,
    )
    .await?;
    Ok(output.expect("ephemeral execution always completes with an output"))
}

/// A writer acknowledges a checkpoint before execution advances.
pub trait CheckpointWriter<S> {
    /// Failure returned when a write cannot be acknowledged.
    type Error;

    /// Acknowledge a checkpoint once the write has succeeded.
    fn write(
        &mut self,
        checkpoint: &Checkpoint<S>,
    ) -> impl Future<Output = Result<(), Self::Error>>;
}

struct DiscardCheckpoint;

impl<S> CheckpointWriter<S> for DiscardCheckpoint {
    type Error = Infallible;

    async fn write(&mut self, _: &Checkpoint<S>) -> Result<(), Self::Error> {
        Ok(())
    }
}

struct OwnedCallback<F>(F);

impl<S, C, F, Save> CheckpointWriter<S> for OwnedCallback<F>
where
    S: Clone,
    F: FnMut(Checkpoint<S>) -> Save,
    Save: Future<Output = Result<(), C>>,
{
    type Error = C;

    fn write(&mut self, checkpoint: &Checkpoint<S>) -> impl Future<Output = Result<(), C>> {
        (self.0)(checkpoint.clone())
    }
}

/// Failure while advancing a workflow.
#[derive(Debug, thiserror::Error)]
pub enum RunError<E, C> {
    /// The cancellation token was triggered.
    #[error("workflow was cancelled")]
    Cancelled,
    /// The execution segment exhausted its time budget.
    #[error("workflow execution timed out")]
    TimedOut,
    /// A ready checkpoint has exhausted its cumulative step budget.
    #[error("workflow step limit reached")]
    StepLimitReached,
    /// The step callback returned an error.
    #[error("workflow step failed: {0}")]
    Step(E),
    /// The checkpoint writer returned an error.
    #[error("checkpoint commit failed: {0}")]
    Commit(C),
}

/// A workflow error when checkpoints are discarded or infallible.
pub type SimpleRunError<E> = RunError<E, Infallible>;

/// A workflow definition whose steps and checkpoint commits can be driven by [`run_workflow`].
pub trait Workflow<S>: Send + Sync {
    /// Failure returned by step execution or checkpoint commit.
    type Error;

    /// Execute the current step and return the next position.
    fn step(&self, state: S) -> impl Future<Output = Result<Position<S>, Self::Error>> + Send;

    /// Acknowledge or persist a checkpoint before advancing. Defaults to a no-op.
    fn commit(
        &self,
        _checkpoint: &Checkpoint<S>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send {
        async { Ok(()) }
    }
}

/// Run a workflow until completion, pause, or error using the [`Workflow`] trait.
///
/// Each transition is committed via [`Workflow::commit`] before the next step starts.
pub async fn run_workflow<S, W>(
    checkpoint: &mut Checkpoint<S>,
    limits: RunLimits,
    cancel: &CancellationToken,
    workflow: &W,
) -> Result<RunOutcome, RunError<W::Error, W::Error>>
where
    S: Clone,
    W: Workflow<S>,
{
    run_with_checkpoints(
        checkpoint,
        limits,
        cancel,
        |state| workflow.step(state),
        WorkflowWriter(workflow),
    )
    .await
}

struct WorkflowWriter<'a, W>(&'a W);

impl<S, W> CheckpointWriter<S> for WorkflowWriter<'_, W>
where
    W: Workflow<S>,
{
    type Error = W::Error;

    fn write(
        &mut self,
        checkpoint: &Checkpoint<S>,
    ) -> impl Future<Output = Result<(), Self::Error>> {
        self.0.commit(checkpoint)
    }
}

/// Run until completion, pause, or error. Each transition is committed before
/// the next step starts. A failed commit leaves the supplied cursor at its last
/// acknowledged position.
///
/// Callers own atomicity, deduplication and recovery for external effects. If a
/// commit is interrupted or its acknowledgement is lost, reload the checkpoint
/// from the store before retrying.
///
/// If a write returns success before the executor observes cancellation or a
/// deadline overrun, the checkpoint advances. The invocation still returns
/// `Cancelled` or `TimedOut` once it observes that condition, even if the
/// acknowledged transition paused or completed the workflow.
///
/// # Parameters
///
/// - `checkpoint` is the cursor to advance from its last acknowledged position.
/// - `limits` bounds cumulative steps and the time available to this invocation.
/// - `cancel` lets the caller interrupt the current step or write cooperatively.
/// - `step` receives an owned clone of the ready state and returns the next position.
/// - `commit` receives an owned checkpoint for each successful transition,
///   including the transition into a paused or completed position.
///
/// A paused or completed checkpoint returns its current outcome without
/// invoking either callback. Use [`Checkpoint::resume`] or
/// [`Checkpoint::resume_with`] to make a paused checkpoint ready again.
///
/// # Errors
///
/// Returns [`RunError`] for cancellation, timeout, exhausted steps, or an
/// error from either callback.
pub async fn run<S, E, C, Step, Work, Commit, Save>(
    checkpoint: &mut Checkpoint<S>,
    limits: RunLimits,
    cancel: &CancellationToken,
    step: Step,
    commit: Commit,
) -> Result<RunOutcome, RunError<E, C>>
where
    S: Clone,
    Step: FnMut(S) -> Work,
    Work: Future<Output = Result<Position<S>, E>>,
    Commit: FnMut(Checkpoint<S>) -> Save,
    Save: Future<Output = Result<(), C>>,
{
    run_with_checkpoints(checkpoint, limits, cancel, step, OwnedCallback(commit)).await
}

/// Execute with the same parameters and outcomes as [`run`], using a writer
/// that borrows each checkpoint in place of the owned `commit` callback.
///
/// A writer must acknowledge each checkpoint before the cursor advances.
/// See [`run`] for commit and recovery semantics.
pub async fn run_with_checkpoints<S, E, Work, Writer>(
    checkpoint: &mut Checkpoint<S>,
    limits: RunLimits,
    cancel: &CancellationToken,
    mut step: impl FnMut(S) -> Work,
    writer: Writer,
) -> Result<RunOutcome, RunError<E, Writer::Error>>
where
    S: Clone,
    Work: Future<Output = Result<Position<S>, E>>,
    Writer: CheckpointWriter<S>,
{
    run_inner(
        checkpoint,
        limits,
        cancel,
        |state| step(state).map(|result| result.map(|position| (position, None::<()>))),
        writer,
    )
    .await
    .map(|(outcome, _)| outcome)
}

async fn run_inner<S, O, E, Work, Writer>(
    checkpoint: &mut Checkpoint<S>,
    limits: RunLimits,
    cancel: &CancellationToken,
    mut step: impl FnMut(S) -> Work,
    mut writer: Writer,
) -> Result<(RunOutcome, Option<O>), RunError<E, Writer::Error>>
where
    S: Clone,
    Work: Future<Output = Result<(Position<S>, Option<O>), E>>,
    Writer: CheckpointWriter<S>,
{
    let started = tokio::time::Instant::now();
    // The outer select can interrupt a pending future. Check at callback
    // boundaries too, in case a callback finishes without yielding.
    let check_interrupted = || {
        if cancel.is_cancelled() {
            return Err(RunError::Cancelled);
        }
        if started.elapsed() >= limits.timeout {
            return Err(RunError::TimedOut);
        }
        Ok(())
    };
    let execution = async {
        loop {
            let state = match &checkpoint.position {
                Position::Ready(state) => state,
                Position::Paused(_) => return Ok((RunOutcome::Paused, None)),
                Position::Completed => return Ok((RunOutcome::Completed, None)),
            };
            if checkpoint.completed_steps >= limits.max_steps.get() {
                return Err(RunError::StepLimitReached);
            }
            // Give the executor a poll boundary even when both callbacks are ready.
            tokio::task::yield_now().await;
            check_interrupted()?;
            let result = step(state.clone()).await;
            check_interrupted()?;
            let (position, output) = result.map_err(RunError::Step)?;
            let next = Checkpoint {
                position,
                completed_steps: checkpoint.completed_steps + 1,
            };
            writer.write(&next).await.map_err(RunError::Commit)?;
            *checkpoint = next;
            check_interrupted()?;
            if output.is_some() {
                return Ok((RunOutcome::Completed, output));
            }
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
///
/// `items` supplies jobs lazily, `concurrency` caps simultaneously active job
/// futures, and `job` runs one item. The batch inherits cancellation and timeout
/// when awaited inside [`run`].
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
    stream::iter(items)
        .then(|item| async {
            tokio::task::consume_budget().await;
            Ok(item)
        })
        .try_for_each_concurrent(concurrency.get(), job)
        .await
}
