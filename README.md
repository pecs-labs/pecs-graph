# pecs-graph

[English](README.en.md)

`pecs-graph` 是一个异步 Rust 工作流执行库。每个步骤可以通过强类型状态指定下一步、暂停并保存继续位置，或结束工作流。调用方提供业务逻辑，并决定如何存储检查点和执行结果。

## 运行工作流

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

## 检查点与恢复

- 执行器在推进到下一步前，将新检查点交给提交回调。临时执行可以传入空操作回调。需要持久化时，先保存初始检查点，再通过回调持久化后续检查点。
- 步骤返回 `Position::Paused(next)` 时，执行器提交暂停状态后返回。收到外部输入后，加载检查点并调用 `Checkpoint::resume`。将恢复后的检查点和输入一并保存，再调用 `run`。`Checkpoint::resume` 会保留累计步骤数。
- `run_batch` 限制并发数。为批次任务分配稳定 ID，并在每项完成时提交结果。重启后加载已提交结果，只将尚未完成的任务传给批次执行器。如果步骤完成后、检查点确认前发生故障，恢复时可能会重试该步骤，因此外部操作应支持幂等。
- 持久化记录应包含工作流标识和状态结构版本。存储事务、执行权归属和持久化状态升级由应用负责。

运行 `cargo test` 可独立验证执行器。
