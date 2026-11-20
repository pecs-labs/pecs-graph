# 基于 pecs-graph 构建业务工作流开发指南

本文档介绍如何在智囊团（Roundtable）之外，为新业务（如：多维度代码审查、研报分析、风控审核、AI 会话编排等）快速接入 `pecs-graph` 工作流。

---

## 工作流从哪读起

先读步骤 enum 和执行一步的 `match`。enum 表示某一步需要的输入，`match` 执行当前步骤并用返回值指明下一步。几个操作总是一起完成时，可以放在同一步内。需要独立保存进度、暂停或恢复时，再为它定义单独的步骤。

```mermaid
flowchart LR
    A["当前步骤"] --> B["执行本步的业务工作"]
    B --> C["返回下一位置"]
    C --> D["执行器确认提交"]
    D -->|Ready| A
    D -->|Paused| E["等外部输入"]
    D -->|Completed| F["结束"]
```

把步骤和业务分支放在一起，固定的下一步直接写在分支里。动态流程可以从业务状态或模型决策选择下一步，最终仍返回 `Ready`、`Paused` 或 `Completed`。运行入口负责加载状态、设置上限和调用执行器，检查点保存及用户恢复可以放在同一个文件的后部。

| 需要什么 | 用哪个入口 |
| --- | --- |
| 进程内执行并返回结果 | `execute` 或 `execute_with_limits` |
| 自定义检查点保存、暂停或取消 | `run`，直接传入步骤和提交回调 |
| 同一个工作流对象提供步骤和提交方法 | 实现 `Workflow`，交给 `run_workflow` 执行 |
| 通过运行 ID 加载并提交检查点 | 实现持久化 `Store`，调用 `run_stored` |

---

## 快速接入模版：以「智能代码审查服务」为例

下面是一个完整的业务骨架示例：主 Agent 规划审查维度，并行执行子任务检查，若发现高危安全风险则暂停等待人工确认（Human-in-the-loop），确认后继续汇总生成报告。

### 1. 声明步骤状态机与业务快照

```rust
use pecs_graph::{
    CancellationToken, Checkpoint, Position, RunLimits, RunOutcome, Workflow, run, run_batch,
};
use serde::{Deserialize, Serialize};
use std::{num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::sync::Mutex;

// 1. 显式步骤枚举：携带下一步需要的数据
// 建议 derive Clone, Debug, PartialEq 以及 Serialize, Deserialize（便于持久化和日志）
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum ReviewStep {
    /// 初始状态：准备规划
    Start,
    /// 规划阶段：分解审查任务
    Plan,
    /// 并发子任务阶段：携带待执行任务列表
    Inspect(Vec<ReviewTask>),
    /// 暂停等待人工介入：高危风险确认
    AwaitApproval { high_risks: Vec<String> },
    /// 汇总阶段：生成最终审查报告
    Synthesize,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ReviewTask {
    pub task_id: String,
    pub dimension: String, // e.g. "security", "performance", "style"
    pub prompt: String,
}

// 2. 业务快照：持久化上下文
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReviewSnapshot {
    pub review_id: String,
    pub version: u64,
    pub status: String, // "running", "awaiting_approval", "completed", "failed"
    pub code_diff: String,
    pub task_results: Vec<(String, String)>, // (task_id, result)
    pub approved_by_user: bool,
    pub final_report: Option<String>,
}
```

---

### 2. 实现单步分发与执行逻辑 (`execute_step`)

`execute_step` 是流程的交通枢纽，每一步完成后返回：
- `Position::Ready(next_step)`：进入下一步；
- `Position::Paused(step)`：暂停当前流程，等待外部事件/用户输入唤醒；
- `Position::Completed`：流程正常完结。

```rust
pub struct ReviewService {
    // 示例中的单个运行
    pub state: Arc<Mutex<ReviewState>>,
}

pub struct ReviewState {
    pub snapshot: ReviewSnapshot,
    pub checkpoint: Checkpoint<ReviewStep>,
    pub cancel: CancellationToken,
}

impl ReviewService {
    /// 单步业务执行路由
    async fn execute_step(
        &self,
        id: &str,
        step: ReviewStep,
    ) -> Result<Position<ReviewStep>, MyError> {
        match step {
            ReviewStep::Start => {
                println!("[{id}] 步骤 1: 初始化工作流...");
                Ok(Position::Ready(ReviewStep::Plan))
            }

            ReviewStep::Plan => {
                println!("[{id}] 步骤 2: 主模型规划审查维度...");
                // 模拟模型调用规划出的子任务
                let tasks = vec![
                    ReviewTask {
                        task_id: "sec".into(),
                        dimension: "security".into(),
                        prompt: "检查 SQL 注入与鉴权绕过".into(),
                    },
                    ReviewTask {
                        task_id: "perf".into(),
                        dimension: "performance".into(),
                        prompt: "检查内存分配与 O(N^2) 循环".into(),
                    },
                ];
                Ok(Position::Ready(ReviewStep::Inspect(tasks)))
            }

            ReviewStep::Inspect(tasks) => {
                println!("[{id}] 步骤 3: 并行执行子审查任务，批次大小: {}", tasks.len());
                let concurrency = NonZeroUsize::new(4).unwrap();
                
                // 使用 run_batch 实现有界并发与异常快速中断
                run_batch(&tasks, concurrency, |task| async move {
                    // 调用各个细分 Agent / LLM
                    let output = if task.task_id == "sec" {
                        "发现疑似密钥硬编码".into()
                    } else {
                        format!("{} 维度检查通过，无明显缺陷", task.dimension)
                    };
                    self.save_task_result(id, &task.task_id, output).await;
                    Ok::<_, MyError>(())
                }).await?;

                // 检查是否命中高危条件需要人工介入
                let has_high_risk = self.state.lock().await.snapshot.task_results.iter()
                    .any(|(_, result)| result == "发现疑似密钥硬编码");
                if has_high_risk {
                    Ok(Position::Paused(ReviewStep::AwaitApproval {
                        high_risks: vec!["发现疑似密钥硬编码".into()],
                    }))
                } else {
                    Ok(Position::Ready(ReviewStep::Synthesize))
                }
            }

            ReviewStep::AwaitApproval { .. } => {
                // 当通过 resume 唤醒后，直接进入汇总阶段
                println!("[{id}] 步骤 4: 人工已确认，继续流程...");
                Ok(Position::Ready(ReviewStep::Synthesize))
            }

            ReviewStep::Synthesize => {
                println!("[{id}] 步骤 5: 汇总所有分析并生成最终报告...");
                self.finalize_report(id).await?;
                Ok(Position::Completed)
            }
        }
    }

    async fn save_task_result(&self, _id: &str, task_id: &str, res: String) {
        let mut s = self.state.lock().await;
        s.snapshot.task_results.push((task_id.to_string(), res));
        s.snapshot.version += 1;
    }

    async fn finalize_report(&self, _id: &str) -> Result<(), MyError> {
        let mut s = self.state.lock().await;
        s.snapshot.final_report = Some("代码质量评估：良好。".into());
        s.snapshot.version += 1;
        Ok(())
    }
}
```

---

### 3. 实现检查点提交 (`commit_checkpoint`)

执行器完成提交后才启动下一步。这个内存示例在同一把锁内更新检查点和对外状态。步骤执行期间保存的任务结果如需跨进程恢复，还要有独立的持久化和去重策略。

```rust
impl ReviewService {
    async fn commit_checkpoint(
        &self,
        id: &str,
        checkpoint: Checkpoint<ReviewStep>,
    ) -> Result<(), MyError> {
        let mut s = self.state.lock().await;
        if s.cancel.is_cancelled() {
            return Err(MyError::Cancelled);
        }

        // 处理暂停点状态（Human-in-the-loop）
        match checkpoint.position() {
            Position::Paused(ReviewStep::AwaitApproval { high_risks }) => {
                s.snapshot.status = "awaiting_approval".into();
                println!("[{id}] 流程暂停等待人工介入: {high_risks:?}");
            }
            Position::Completed => s.snapshot.status = "completed".into(),
            _ => {}
        }

        s.checkpoint = checkpoint;
        s.snapshot.version += 1;
        Ok(())
    }
}
```

---

### 4. 驱动后台任务与人机交互恢复 (`drive & resume`)

`pecs-graph` 支持两种驱动方式：

#### 方式 A：实现 `Workflow` Trait 驱动

为业务服务实现 `Workflow<Step>`，将步骤执行与检查点提交统一聚合：

```rust
impl Workflow<ReviewStep> for ReviewService {
    type Error = MyError;

    async fn step(&self, state: ReviewStep) -> Result<Position<ReviewStep>, Self::Error> {
        self.execute_step("default_run", state).await
    }

    async fn commit(&self, checkpoint: &Checkpoint<ReviewStep>) -> Result<(), Self::Error> {
        self.commit_checkpoint("default_run", checkpoint.clone()).await
    }
}
```

驱动调用只需传递 `&self`，签名清晰，避免裸闭包重复样板：

```rust
// 使用 run_workflow 驱动
let outcome = pecs_graph::run_workflow(&mut checkpoint, limits, &cancel, self).await;
```

#### 方式 B：闭包组合驱动

当单例 Service 需要管理多租户/多会话、需要将 `id` 等上下文通过闭包捕获注入时，直接使用 `run`：

```rust
impl ReviewService {
    /// 核心主循环驱动入口
    pub async fn drive(&self, id: &str) -> Result<(), MyError> {
        let (mut checkpoint, cancel) = {
            let s = self.state.lock().await;
            (s.checkpoint.clone(), s.cancel.clone())
        };

        let limits = RunLimits {
            max_steps: NonZeroUsize::new(20).unwrap(), // 防死循环
            timeout: Duration::from_secs(60),          // 本次执行片段超时
        };

        let outcome = run(
            &mut checkpoint,
            limits,
            &cancel,
            |step| self.execute_step(id, step),
            |next| self.commit_checkpoint(id, next),
        )
        .await;

        match outcome {
            Ok(RunOutcome::Completed) => println!("[{id}] 工作流执行完毕"),
            Ok(RunOutcome::Paused) => println!("[{id}] 工作流已安全暂停"),
            Err(pecs_graph::RunError::Cancelled) => return Err(MyError::Cancelled),
            Err(pecs_graph::RunError::Step(err) | pecs_graph::RunError::Commit(err)) => {
                return Err(err);
            }
            Err(err) => return Err(MyError::ExecutionFailed(format!("{err:?}"))),
        }
        Ok(())
    }

    /// 用户确认接口（Human-in-the-loop 唤醒）
    pub async fn approve_and_resume(
        self: &Arc<Self>,
        id: &str,
        expected_version: u64,
    ) -> Result<(), MyError> {
        {
            let mut s = self.state.lock().await;
            if s.snapshot.version != expected_version
                || !matches!(
                    s.checkpoint.position(),
                    Position::Paused(ReviewStep::AwaitApproval { .. })
                )
            {
                return Err(MyError::InvalidState);
            }
            s.snapshot.approved_by_user = true;
            s.snapshot.status = "running".into();
            s.snapshot.version += 1;
            // 关键：将 Paused 游标转为 Ready
            s.checkpoint = s.checkpoint.clone().resume();
        }

        // 后台重新调度驱动
        let svc = self.clone();
        let run_id = id.to_string();
        tokio::spawn(async move {
            if let Err(err) = svc.drive(&run_id).await {
                eprintln!("[{run_id}] 工作流异常退出: {err:?}");
            }
        });

        Ok(())
    }
}

#[derive(Debug)]
pub enum MyError {
    Cancelled,
    InvalidState,
    ExecutionFailed(String),
}
```

---

## 常见设计模式与技巧

### 1. 循环与反思模式（Reflection / Self-Correction）
如果需要类似 “生成 $\to$ 验证 $\to$ 不合格重新修改” 的循环：
- 在 `ReviewStep` 中定义 `Refine { round: u8 }`；
- 在 `execute_step` 中判断 `round < max_round` 则返回 `Position::Ready(ReviewStep::Refine { round: round + 1 })`；
- 结合 `pecs_graph::RunLimits { max_steps, .. }`，框架层能保证不会出现死循环。

### 2. 外部持久化升级（跨进程 / 分布式恢复）

1. 用持久化后端实现 `Store::load` 和 `Store::compare_and_put`。后者须原子地比较版本并写入。用 `compare_and_put(key, None, &Checkpoint::new(initial))` 创建初始检查点。
2. 按固定的运行标识调用 `run_stored`。每次调用都从存储加载最新检查点，使用当前版本提交每一个新位置。
3. 需要人工输入时，让步骤返回 `Position::Paused(next)`，并将暂停记录的 revision 随输入请求一起提供给调用方。收到回复后加载最新检查点，确认它仍处于暂停且版本与请求一致。用 `resume_with` 合入经业务校验的输入，再用该版本提交。收到 `StoreError::Conflict` 时重新读取状态，让调用方确认最新请求。

运行 `cargo run -p pecs-graph --example stored_approval` 可以体验[人工审批示例](examples/stored_approval.rs)。它使用 `MemoryStore` 演示暂停、附带版本提交回复、拒绝过期回复和续跑。

版本比较发生在提交时。两个 worker 可以同时读到同一版本，各自执行一步，随后其中一个写入失败。需要同一运行只有一个活跃执行者时，由调度层协调运行权。如果步骤可能重做，用可从持久化状态重建的幂等键调用支持去重的外部服务。确认丢失时，先查询已保存的检查点和外部操作结果，再决定是否重试。

### 3. 取消与快速熔断

检查点写入也计入本次执行的时间预算。执行器在步骤和写入之间检查取消与超时，并可停止等待挂起的 future。需要在子任务里结束工作时，将取消信号传给子任务。已经发给外部服务的请求仍可能完成。

| 写入结果 | 本地检查点 | 恢复方式 |
| --- | --- | --- |
| 写入已确认，随后检测到超时或取消 | 保留新位置，包括暂停和完成 | 从该位置恢复。持久化执行可以重新加载该位置 |
| 等待确认时超时、取消或后端返回错误 | 保留上一次确认的位置 | 持久化后端可能已经提交。重试前从后端重新加载 |

### 4. 动态规划与执行循环（Plan-Execute Loop）
在智能体决策工作流中（如 Roundtable 智囊团），步骤往往不是线性的，而是由规划模型动态主导：
- `Start -> Plan`：主持 Agent 读取当前上下文并输出下一步任务拆分；
- `Plan -> Analyze(tasks)`：并发执行子 Agent 分析；
- `Analyze -> Plan`：子任务执行完后**重新回到 Plan**，让规划模型感知子任务产物并决定是否需要补充交叉评审（Review）或直接汇总；
- `Plan -> Synthesize`：满足收敛条件后进入汇总完结。
通过 `RunLimits::max_steps` 可从容限制总轮数，无需担心模型死循环。

### 5. 部分失败容忍与优雅降级（Partial Failure Tolerance）
并发调用 LLM 或外部微服务时，常发生网络超时、偶发风控拦截或限流：
- **不要直接中断整个工作流**：在 `run_batch` 的闭包内部捕获单任务异常，将错误状态记录在快照的 `tasks` 中；
- **降级收敛**：在步骤流转时检查是否仍有可用结果（例如 `snapshot.tasks.iter().any(|t| t.result.is_some())`）；若有则继续流转到汇总步骤并在 Prompt 中注明部分失败，确保为终端用户产出降级后的最大价值，而非直接返回空白错误。

### 6. 批处理模式选型指南 (`run_batch` vs `run_stored_batch`)
| 维度 | `run_batch` | `run_stored_batch` |
|:---|:---|:---|
| **适用场景** | 内存快照型工作流、短时任务、结果合并写回 Snapshot | 跨进程/分布式任务、长时外部调用、强断点续跑 |
| **持久化粒度** | 外部统一在 Step/Snapshot 层面持久化 | 每个 Task 根据 `TaskKey(run_id, batch_id, task_id)` 独立持久化 |
| **幂等机制** | 外部业务层管理幂等过滤 | 框架自动查询 Store，已完成任务跳过执行直接重用结果 |
| **重试行为** | 由调用方读取已保存结果并筛出待执行项 | 重试使用相同的运行、批次和任务 ID 及输入。只复用已保存的结果，未保存结果的任务可能重做 |

### 7. 检查点写入与副作用分离
`CheckpointWriter::write` 或 `commit_checkpoint` 的首要职责是保证游标原子提交：
- **纯粹性**：建议尽量只执行数据库更新或乐观锁版本递增；
- **状态发布**：WebSocket/SSE 实时广播、计时统计等业务副作用，建议放在 `commit` 成功后的外部事件通道中发布，避免在提交事务内包含可能阻塞或失败的网络副作用。

### 8. 错误类型别名与序列化最佳实践
- 当步骤无需持久化或 checkpoint 提交永远成功时，使用 `SimpleRunError<E>`（等价于 `RunError<E, Infallible>`）简化函数签名；
- 步骤枚举推荐派生 `#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]`。如果包含动态原因或字符串，优先使用 `String` 而非 `&'static str`，以确保能够跨进程序列化恢复。
