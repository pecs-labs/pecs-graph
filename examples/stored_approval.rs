use std::{convert::Infallible, error::Error};

use pecs_graph::{
    CancellationToken, Checkpoint, MemoryStore, Position, RunLimits, RunOutcome, Store, StoreError,
    run_stored,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
enum ReviewStep {
    RequestApproval,
    AwaitDecision,
    ApplyDecision(bool),
    VerifyApproved,
}

async fn step(state: ReviewStep) -> Result<Position<ReviewStep>, Infallible> {
    Ok(match state {
        ReviewStep::RequestApproval | ReviewStep::AwaitDecision => {
            Position::Paused(ReviewStep::AwaitDecision)
        }
        ReviewStep::ApplyDecision(true) => Position::Ready(ReviewStep::VerifyApproved),
        ReviewStep::ApplyDecision(false) | ReviewStep::VerifyApproved => Position::Completed,
    })
}

async fn submit_decision<K, D>(
    store: &D,
    run_id: &K,
    expected_revision: u64,
    approved: bool,
) -> Result<bool, StoreError<D::Error>>
where
    D: Store<K, Checkpoint<ReviewStep>>,
{
    let Some(current) = store.load(run_id).await.map_err(StoreError::Backend)? else {
        return Ok(false);
    };
    if current.revision != expected_revision
        || !matches!(
            current.value.position(),
            Position::Paused(ReviewStep::AwaitDecision)
        )
    {
        return Ok(false);
    }

    let ready = current
        .value
        .resume_with(|_| ReviewStep::ApplyDecision(approved));
    match store
        .compare_and_put(run_id, Some(expected_revision), &ready)
        .await
    {
        Ok(_) => Ok(true),
        Err(StoreError::Conflict) => Ok(false),
        Err(error) => Err(error),
    }
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn Error>> {
    // Use a durable Store implementation to survive a process restart.
    let store = MemoryStore::default();
    let run_id = "review-42";
    let limits = RunLimits::default();
    let cancel = CancellationToken::new();

    store
        .compare_and_put(&run_id, None, &Checkpoint::new(ReviewStep::RequestApproval))
        .await?;
    let paused = run_stored(&store, &run_id, limits, &cancel, step).await?;
    assert_eq!(paused, RunOutcome::Paused);

    // Include this revision in the approval request shown to the reviewer.
    let revision = store.load(&run_id).await?.expect("review exists").revision;
    assert!(submit_decision(&store, &run_id, revision, true).await?);
    assert!(!submit_decision(&store, &run_id, revision, false).await?);

    let completed = run_stored(&store, &run_id, limits, &cancel, step).await?;
    assert_eq!(completed, RunOutcome::Completed);
    println!("Review completed. The stale decision was rejected.");
    Ok(())
}
