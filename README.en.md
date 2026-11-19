# pecs-graph

[简体中文](README.md)

`pecs-graph` runs async Rust workflows with typed step transitions. A step can select
another step, pause with a continuation, or complete. Callers provide workflow
logic and decide how to store checkpoints and results.

## Run a workflow

```rust
use std::{convert::Infallible, future, num::NonZeroUsize, time::Duration};

use pecs_graph::{CancellationToken, Checkpoint, Position, RunLimits, RunOutcome, run};

#[derive(Clone)]
enum Step {
    Validate,
    Publish,
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut checkpoint = Checkpoint::new(Step::Validate);
    let outcome = run(
        &mut checkpoint,
        RunLimits {
            max_steps: NonZeroUsize::new(4).unwrap(),
            timeout: Duration::from_secs(10),
        },
        &CancellationToken::new(),
        |step| async move {
            Ok::<_, Infallible>(match step {
                Step::Validate => Position::Ready(Step::Publish),
                Step::Publish => Position::Completed,
            })
        },
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Completed);
}
```

## Checkpoints and recovery

- The commit callback receives each new checkpoint before execution advances.
  Use a no-op callback for transient runs. For durable runs, save the initial
  checkpoint before starting and persist each subsequent checkpoint in the callback.
- When a step returns `Position::Paused(next)`, the run returns after committing
  the pause. Load the checkpoint, accept the external input, and persist the
  resumed checkpoint together with that input before calling `run` again.
  `Checkpoint::resume` retains the cumulative step count.
- `run_batch` bounds concurrency. Give items stable IDs and commit each result
  as it completes. After a restart, load the results and pass only pending items
  to the batch. Make external effects idempotent because a step can be retried
  after it finishes work but before its checkpoint is acknowledged.
- Store workflow identity and state schema version with durable records.
  The application owns storage transactions, worker ownership, and upgrades
  of persisted workflow state.

Run `cargo test` to exercise the executor independently.
