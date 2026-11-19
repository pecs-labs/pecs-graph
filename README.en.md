# pecs-graph

[简体中文](README.md) · [Developer Guide](GUIDE.md)

`pecs-graph` runs async Rust workflows with typed step transitions. A step can select
another step, pause with a continuation, or complete. Callers provide workflow
logic and decide how to store checkpoints and results.

## Run a workflow

### Use defaults

`execute(step)` takes the step function. The initial state comes from the
state type's `Default` implementation. For example, `u32` starts at `0`.
The execution allows up to 128 committed steps and times out after 30 seconds.

```rust
use std::convert::Infallible;
use pecs_graph::{Step, execute};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let result = execute(|count: u32| async move {
        Ok::<_, Infallible>(if count < 2 {
            Step::Continue(count + 1)
        } else {
            Step::Complete(format!("count = {count}"))
        })
    })
    .await
    .unwrap();
    assert_eq!(result, "count = 2");
}
```

### Set the initial state and execution limits

`execute_with_limits(initial, limits, step)` takes the initial state, the
execution limits, and the step function. This example starts at `7`,
allows up to 4 committed steps, and times out after 10 seconds.

```rust
use std::{convert::Infallible, num::NonZeroUsize, time::Duration};
use pecs_graph::{RunLimits, Step, execute_with_limits};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let limits = RunLimits {
        max_steps: NonZeroUsize::new(4).unwrap(),
        timeout: Duration::from_secs(10),
    };
    let result = execute_with_limits(7u32, limits, |count| async move {
        Ok::<_, Infallible>(if count < 8 {
            Step::Continue(count + 1)
        } else {
            Step::Complete(count)
        })
    })
    .await
    .unwrap();
    assert_eq!(result, 8);
}
```

To override only the timeout, use
`RunLimits { timeout: Duration::from_secs(10), ..RunLimits::default() }`.
To customize only the initial state, pass `RunLimits::default()` as the
second argument.

## Checkpoints and recovery

`run(checkpoint, limits, cancel, step, commit)` advances a workflow from its
checkpoint. A step counts once its transition is committed, including a
transition that pauses or completes. A step can contain several operations,
such as parallel model calls. Applications can limit model calls separately.

The example uses a no-op callback for in-process pause and resume. To make
recovery durable, save the checkpoint in `commit`. Each resume receives a
fresh timeout and keeps the checkpoint's cumulative step count.

```rust
use std::{convert::Infallible, future};
use pecs_graph::{CancellationToken, Checkpoint, Position, RunLimits, RunOutcome, run};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let mut checkpoint = Checkpoint::new(0u32);
    let limits = RunLimits::default();
    let cancel = CancellationToken::new();
    let step = |state| future::ready(Ok::<_, Infallible>(if state == 0 {
        Position::Paused(1)
    } else {
        Position::Completed
    }));
    let commit = |_| future::ready(Ok::<_, Infallible>(()));

    let outcome = run(&mut checkpoint, limits, &cancel, step, commit).await.unwrap();
    assert_eq!(outcome, RunOutcome::Paused);

    checkpoint = checkpoint.resume();
    let outcome = run(&mut checkpoint, limits, &cancel, step, commit).await.unwrap();
    assert_eq!(outcome, RunOutcome::Completed);
    assert_eq!(checkpoint.completed_steps(), 2);
}
```

- Use `execute` for transient workflows with typed results. Use `run` or its
  borrowing counterpart `run_with_checkpoints` for custom commits, pauses,
  and cancellation.
- Create the initial checkpoint with `Store::compare_and_put` and an expected
  revision of `None`. Then call `run_stored` with the same run key. To resume a
  pause, load the checkpoint, apply the input with `resume_with`, and commit
  against the loaded revision. The cumulative step count carries forward.
- `run_stored_batch` saves outputs by run, batch, and task ID. Repeat the same
  IDs to reuse committed outputs. A restart can repeat work whose output was
  still pending, so make external task effects idempotent.
- `MemoryStore` supports tests and in-process resume. Implement a durable
  `Store` for cross-process recovery. Conditional writes protect revisions,
  and applications coordinate ownership when scheduling across workers.

Run `cargo test` to exercise the executor independently.
