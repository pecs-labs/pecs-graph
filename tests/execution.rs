use std::{cell::RefCell, convert::Infallible, future, num::NonZeroUsize, time::Duration};

use pecs_graph::{CancellationToken, Checkpoint, Position, RunError, RunLimits, RunOutcome};

fn limits(max_steps: usize) -> RunLimits {
    RunLimits {
        max_steps: NonZeroUsize::new(max_steps).unwrap(),
        timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn resumes_a_serialized_pause_and_preserves_the_step_count() {
    let saved = RefCell::new(Vec::new());
    let cancel = CancellationToken::new();
    let outcome = pecs_graph::run(
        &mut Checkpoint::new(0),
        limits(2),
        &cancel,
        |step| async move {
            assert_eq!(step, 0);
            Ok::<_, Infallible>(Position::Paused(1))
        },
        |checkpoint| future::ready(serde_json::to_writer(&mut *saved.borrow_mut(), &checkpoint)),
    )
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Paused);

    let paused: Checkpoint<u8> = serde_json::from_slice(&saved.into_inner()).unwrap();
    assert_eq!(paused.position(), &Position::Paused(1));
    assert_eq!(paused.completed_steps(), 1);

    let mut resumed = paused.resume();
    let outcome = pecs_graph::run(
        &mut resumed,
        limits(2),
        &cancel,
        |step| async move {
            assert_eq!(step, 1);
            Ok::<_, Infallible>(Position::Completed)
        },
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Completed);
    assert_eq!(resumed.completed_steps(), 2);
}

#[tokio::test]
async fn failed_commit_keeps_the_last_acknowledged_position() {
    let mut checkpoint = Checkpoint::new(0);
    let result = pecs_graph::run(
        &mut checkpoint,
        limits(4),
        &CancellationToken::new(),
        |step| future::ready(Ok::<_, Infallible>(Position::Ready(step + 1))),
        |checkpoint| {
            future::ready(if checkpoint.completed_steps() == 2 {
                Err("store unavailable")
            } else {
                Ok(())
            })
        },
    )
    .await;
    assert!(matches!(result, Err(RunError::Commit("store unavailable"))));
    assert_eq!(checkpoint.position(), &Position::Ready(1));
    assert_eq!(checkpoint.completed_steps(), 1);
}

#[tokio::test]
async fn immediate_loop_stops_at_step_limit() {
    let mut checkpoint = Checkpoint::new(());
    let result = pecs_graph::run(
        &mut checkpoint,
        limits(3),
        &CancellationToken::new(),
        |_| future::ready(Ok::<_, Infallible>(Position::Ready(()))),
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await;
    assert!(matches!(result, Err(RunError::StepLimitReached)));
    assert_eq!(checkpoint.completed_steps(), 3);
}

#[tokio::test(start_paused = true)]
async fn timeout_leaves_the_interrupted_step_pending() {
    let mut checkpoint = Checkpoint::new(());
    let result = pecs_graph::run(
        &mut checkpoint,
        limits(3),
        &CancellationToken::new(),
        |_| future::pending::<Result<Position<()>, Infallible>>(),
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await;
    assert!(matches!(result, Err(RunError::TimedOut)));
    assert_eq!(checkpoint.completed_steps(), 0);
}

#[tokio::test]
async fn cancellation_prevents_a_step_from_committing() {
    let cancel = CancellationToken::new();
    let mut checkpoint = Checkpoint::new(());
    let result = pecs_graph::run(
        &mut checkpoint,
        limits(3),
        &cancel,
        |_| {
            cancel.cancel();
            future::ready(Ok::<_, Infallible>(Position::Completed))
        },
        |_| -> future::Ready<Result<(), Infallible>> { panic!("cancelled step must not commit") },
    )
    .await;
    assert!(matches!(result, Err(RunError::Cancelled)));
    assert_eq!(checkpoint.completed_steps(), 0);
}

#[tokio::test]
async fn elapsed_deadline_blocks_commit_after_a_blocking_step() {
    let mut checkpoint = Checkpoint::new(());
    let result = pecs_graph::run(
        &mut checkpoint,
        RunLimits {
            timeout: Duration::from_millis(10),
            ..limits(1)
        },
        &CancellationToken::new(),
        |_| {
            std::thread::sleep(Duration::from_millis(20));
            future::ready(Ok::<_, Infallible>(Position::Completed))
        },
        |_| -> future::Ready<Result<(), Infallible>> {
            panic!("an expired step must not begin a commit")
        },
    )
    .await;
    assert!(matches!(result, Err(RunError::TimedOut)));
    assert_eq!(checkpoint.completed_steps(), 0);
}

#[tokio::test(start_paused = true)]
async fn late_write_reports_timeout_but_keeps_the_acknowledged_position() {
    for position in [Position::Ready(1), Position::Paused(1), Position::Completed] {
        let mut checkpoint = Checkpoint::new(0);
        let limits = limits(1);
        let result = pecs_graph::run(
            &mut checkpoint,
            limits,
            &CancellationToken::new(),
            |state| {
                assert_eq!(state, 0);
                future::ready(Ok::<_, Infallible>(position.clone()))
            },
            |_| async {
                tokio::time::advance(limits.timeout).await;
                Ok::<_, Infallible>(())
            },
        )
        .await;

        assert!(matches!(result, Err(RunError::TimedOut)), "{result:?}");
        assert_eq!(checkpoint.position(), &position);
        assert_eq!(checkpoint.completed_steps(), 1);
    }
}

#[tokio::test]
async fn cancellation_during_an_acknowledged_write_preserves_the_transition() {
    for position in [Position::Ready(1), Position::Paused(1), Position::Completed] {
        let mut checkpoint = Checkpoint::new(0);
        let cancel = CancellationToken::new();
        let result = pecs_graph::run(
            &mut checkpoint,
            limits(1),
            &cancel,
            |state| {
                assert_eq!(state, 0);
                future::ready(Ok::<_, Infallible>(position.clone()))
            },
            |_| {
                cancel.cancel();
                future::ready(Ok::<_, Infallible>(()))
            },
        )
        .await;

        assert!(matches!(result, Err(RunError::Cancelled)), "{result:?}");
        assert_eq!(checkpoint.position(), &position);
        assert_eq!(checkpoint.completed_steps(), 1);
    }
}

#[tokio::test]
async fn ready_batch_yields_to_workflow_cancellation() {
    tokio::spawn(async {
        let cancel = CancellationToken::new();
        let result = pecs_graph::run(
            &mut Checkpoint::new(()),
            limits(1),
            &cancel,
            |_| async {
                pecs_graph::run_batch(0..10_000, NonZeroUsize::MIN, |item| {
                    if item == 0 {
                        cancel.cancel();
                    }
                    assert!(item < 9_999, "the batch should yield before finishing");
                    future::ready(Ok::<_, Infallible>(()))
                })
                .await?;
                Ok(Position::Completed)
            },
            |_| future::ready(Ok::<_, Infallible>(())),
        )
        .await;
        assert!(matches!(
            result,
            Err(RunError::<Infallible, Infallible>::Cancelled)
        ));
    })
    .await
    .unwrap();
}

#[tokio::test(start_paused = true)]
async fn a_reloaded_batch_only_executes_uncommitted_items() {
    let committed = RefCell::new(Vec::new());
    let limit = NonZeroUsize::new(2).unwrap();
    let mut checkpoint = Checkpoint::new(vec![1, 2, 3]);
    let saved_cursor = serde_json::to_vec(&checkpoint).unwrap();
    let cancel = CancellationToken::new();
    let first_run = pecs_graph::run(
        &mut checkpoint,
        limits(3),
        &cancel,
        |items| async {
            pecs_graph::run_batch(items, limit, |item| {
                let committed = &committed;
                async move {
                    if item == 3 {
                        future::pending::<()>().await;
                    }
                    committed.borrow_mut().push(item);
                    Ok::<_, Infallible>(())
                }
            })
            .await?;
            Ok::<_, Infallible>(Position::Completed)
        },
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await;
    assert!(matches!(first_run, Err(RunError::TimedOut)));
    assert_eq!(*committed.borrow(), [1, 2]);

    let saved_results = serde_json::to_vec(&committed.into_inner()).unwrap();
    let committed: Vec<u8> = serde_json::from_slice(&saved_results).unwrap();
    let executed = RefCell::new(Vec::new());
    let mut restored: Checkpoint<Vec<u8>> = serde_json::from_slice(&saved_cursor).unwrap();
    let outcome = pecs_graph::run(
        &mut restored,
        limits(3),
        &cancel,
        |items| async {
            let pending = items.into_iter().filter(|id| !committed.contains(id));
            pecs_graph::run_batch(pending, limit, |item| {
                executed.borrow_mut().push(item);
                future::ready(Ok::<_, Infallible>(()))
            })
            .await?;
            Ok::<_, Infallible>(Position::Completed)
        },
        |_| future::ready(Ok::<_, Infallible>(())),
    )
    .await
    .unwrap();
    assert_eq!(outcome, RunOutcome::Completed);
    assert_eq!(executed.into_inner(), [3]);
}

struct TestWorkflow {
    commits: RefCell<Vec<Position<u32>>>,
}

impl pecs_graph::Workflow<u32> for TestWorkflow {
    type Error = Infallible;

    async fn step(&self, state: u32) -> Result<Position<u32>, Self::Error> {
        Ok(if state < 3 {
            Position::Ready(state + 1)
        } else {
            Position::Completed
        })
    }

    async fn commit(&self, checkpoint: &Checkpoint<u32>) -> Result<(), Self::Error> {
        self.commits.borrow_mut().push(checkpoint.position().clone());
        Ok(())
    }
}

#[tokio::test]
async fn run_workflow_drives_trait_implementation() {
    let workflow = TestWorkflow {
        commits: RefCell::new(Vec::new()),
    };
    let mut checkpoint = Checkpoint::new(0u32);
    let cancel = CancellationToken::new();

    let outcome = pecs_graph::run_workflow(
        &mut checkpoint,
        limits(5),
        &cancel,
        &workflow,
    )
    .await
    .unwrap();

    assert_eq!(outcome, RunOutcome::Completed);
    assert_eq!(checkpoint.completed_steps(), 4);
    assert_eq!(
        *workflow.commits.borrow(),
        vec![
            Position::Ready(1),
            Position::Ready(2),
            Position::Ready(3),
            Position::Completed,
        ]
    );
}
