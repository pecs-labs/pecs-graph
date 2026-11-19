use std::{cell::RefCell, convert::Infallible, num::NonZeroUsize, time::Duration};

use pecs_graph::{
    BatchError, CancellationToken, Checkpoint, MemoryStore, Position, RunLimits, RunOutcome, Step,
    Store, StoreError, StoredRunError, execute, execute_with_limits, run_stored, run_stored_batch,
};

fn limits() -> RunLimits {
    RunLimits {
        max_steps: NonZeroUsize::new(3).unwrap(),
        timeout: Duration::from_secs(1),
    }
}

#[tokio::test]
async fn ephemeral_execution_returns_a_typed_output() {
    let output = execute(async |number: u32| {
        Ok::<_, Infallible>(match number {
            0 => Step::Continue(1),
            _ => Step::Complete(String::from("done")),
        })
    })
    .await
    .unwrap();
    assert_eq!(output, "done");
}

#[tokio::test]
async fn ephemeral_execution_supports_default_state_and_explicit_limits() {
    let output = execute(|number: u32| async move { Ok::<_, Infallible>(Step::Complete(number)) })
        .await
        .unwrap();
    assert_eq!(output, 0);

    let result = execute_with_limits(7, limits(), |number| async move {
        assert!(number >= 7);
        Ok::<Step<_, ()>, Infallible>(Step::Continue(number + 1))
    })
    .await;
    assert!(matches!(
        result,
        Err(pecs_graph::RunError::StepLimitReached)
    ));
}

#[tokio::test]
async fn stored_run_resumes_with_input_and_rejects_a_stale_resume() {
    let store = MemoryStore::default();
    let key = "run";
    let cancel = CancellationToken::new();
    store
        .compare_and_put(&key, None, &Checkpoint::new(0))
        .await
        .unwrap();
    let status = run_stored(&store, &key, limits(), &cancel, async |state| {
        assert_eq!(state, 0);
        Ok::<_, Infallible>(Position::Paused(1))
    })
    .await
    .unwrap();
    assert_eq!(status, RunOutcome::Paused);
    let paused = store.load(&key).await.unwrap().unwrap();
    assert_eq!(paused.value.position(), &Position::Paused(1));
    let resumed = paused.value.resume_with(|state| state + 2);
    store
        .compare_and_put(&key, Some(paused.revision), &resumed)
        .await
        .unwrap();
    assert!(matches!(
        store
            .compare_and_put(&key, Some(paused.revision), &resumed)
            .await,
        Err(StoreError::Conflict)
    ));
    let status = run_stored(&store, &key, limits(), &cancel, async |state| {
        assert_eq!(state, 3);
        Ok::<_, Infallible>(Position::Completed)
    })
    .await
    .unwrap();
    assert_eq!(status, RunOutcome::Completed);
    let completed = store.load(&key).await.unwrap().unwrap();
    assert_eq!(completed.value.completed_steps(), 2);
}

#[tokio::test]
async fn stale_runner_cannot_overwrite_a_committed_transition() {
    let store = MemoryStore::default();
    let key = "run";
    let cancel = CancellationToken::new();
    store
        .compare_and_put(&key, None, &Checkpoint::new(0))
        .await
        .unwrap();
    let barrier = tokio::sync::Barrier::new(2);
    let step = async |state| {
        assert_eq!(state, 0);
        barrier.wait().await;
        Ok::<_, Infallible>(Position::Completed)
    };
    let (first, second) = tokio::join!(
        run_stored(&store, &key, limits(), &cancel, step),
        run_stored(&store, &key, limits(), &cancel, step),
    );
    let (committed, stale) = if first.is_ok() {
        (first, second)
    } else {
        (second, first)
    };
    assert_eq!(committed.unwrap(), RunOutcome::Completed);
    assert!(matches!(
        stale,
        Err(StoredRunError::Run(pecs_graph::RunError::Commit(
            StoreError::Conflict
        )))
    ));
}

#[tokio::test]
async fn batch_reuses_saved_results_and_keeps_input_order() {
    let store = MemoryStore::default();
    let items = || [1, 2, 3].map(|number| (number.to_string(), number));
    let result = run_stored_batch(
        &store,
        "run",
        "batch",
        items(),
        NonZeroUsize::MIN,
        |n| async move {
            if n == 3 {
                Err("interrupted")
            } else {
                Ok(n * 10)
            }
        },
    )
    .await;
    assert!(matches!(result, Err(BatchError::Task { task_id, .. }) if task_id == "3"));

    let seen = RefCell::new(Vec::new());
    let result = run_stored_batch(
        &store,
        "run",
        "batch",
        items(),
        NonZeroUsize::new(2).unwrap(),
        |n| {
            seen.borrow_mut().push(n);
            std::future::ready(Ok::<_, Infallible>(n * 10))
        },
    )
    .await
    .unwrap();
    assert_eq!(result, [10, 20, 30]);
    assert_eq!(seen.into_inner(), [3]);
}

#[tokio::test]
async fn batch_rejects_duplicate_ids_before_starting_any_work() {
    let result = run_stored_batch(
        &MemoryStore::<_, ()>::default(),
        "run",
        "batch",
        [("same".into(), 1), ("same".into(), 2)],
        NonZeroUsize::MIN,
        |_| async {
            panic!("duplicate IDs must be rejected before stepping");
            #[allow(unreachable_code)]
            Ok::<(), Infallible>(())
        },
    )
    .await;
    assert!(matches!(result, Err(BatchError::DuplicateTask(id)) if id == "same"));
}
