# pecs-graph

[English](README.en.md) · [新业务快速接入指南](GUIDE.md)

`pecs-graph` 是一个异步 Rust 工作流执行库。每个步骤可以通过强类型状态指定下一步、暂停并保存继续位置，或结束工作流。调用方提供业务逻辑，并决定如何存储检查点和执行结果。

## 运行工作流

### 使用默认参数

`execute(step)` 只接收步骤函数。初始状态来自状态类型的 `Default` 实现，例如 `u32` 的默认值是 `0`。本次执行最多提交 128 步，超时为 30 秒。

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

### 自定义初始状态和执行上限

`execute_with_limits(initial, limits, step)` 依次接收初始状态、执行上限和步骤函数。下面从 `7` 开始，最多提交 4 步，超时为 10 秒。

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

只需覆盖一项上限时，可以写成 `RunLimits { timeout: Duration::from_secs(10), ..RunLimits::default() }`。如果只需指定初始状态，可以将 `RunLimits::default()` 作为第二个参数。

## 检查点与恢复

`run(checkpoint, limits, cancel, step, commit)` 从检查点推进工作流。每次步骤返回下一位置且提交成功后，累计步数加一，包括暂停和完成。一步可以执行多个业务操作，例如在一个批次里并行调用多个模型。模型调用次数可由应用单独限制。

下面用空操作回调演示进程内暂停和续跑。需要持久化时，在 `commit` 中保存检查点。每次续跑使用新的执行超时，累计步数从检查点接着计入。

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

- `execute` 适用于进程内运行并直接返回强类型结果。需要暂停、取消或自定义提交时，可使用 `run` 或采用借用式写入接口的 `run_with_checkpoints`。
- 用 `Store::compare_and_put` 和预期版本 `None` 创建初始检查点，再按运行 ID 调用 `run_stored`。每个步骤都会按当前版本提交。步骤返回 `Position::Paused(next)` 后，可加载检查点，通过 `resume_with` 合入输入，再按加载到的版本提交。续跑保留累计步骤数。
- `run_stored_batch` 通过运行 ID、批次 ID 和任务 ID 保存每项结果。重试时传入相同 ID，已提交的任务会直接返回保存的结果。并发工作可能在保存前中断，外部操作应支持幂等。
- `MemoryStore` 适用于测试和进程内恢复。跨进程恢复需要应用实现持久化 `Store`。版本校验保护提交，多 worker 调度仍需由应用协调执行权。
- 取消或超时可能发生在写入期间。已确认的提交会推进检查点，即使本次调用返回了取消或超时。写入确认缺失时，先从存储重新加载，再决定如何恢复。

运行 `cargo run -p pecs-graph --example stored_approval` 可以体验带版本检查的[人工审批示例](examples/stored_approval.rs)。运行 `cargo test -p pecs-graph` 可验证执行器。
