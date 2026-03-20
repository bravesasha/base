//! The [`SequencerActor`].

use std::{
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use alloy_rpc_types_engine::PayloadId;
use async_trait::async_trait;
use base_alloy_rpc_types_engine::OpExecutionPayloadEnvelope;
use base_consensus_derive::{AttributesBuilder, PipelineErrorKind};
use base_consensus_engine::{InsertTaskError, SealTaskError, SynchronizeTaskError};
use base_consensus_genesis::RollupConfig;
use base_protocol::{BlockInfo, L2BlockInfo, OpAttributesWithParent};
use tokio::{select, sync::mpsc};
use tokio_util::sync::{CancellationToken, WaitForCancellationFuture};

use crate::{
    UpgradeActivations, PoolActivation,
    CancellableContext, NodeActor, SequencerAdminQuery, UnsafePayloadGossipClient,
    actors::{
        SequencerEngineClient,
        engine::EngineClientError,
        sequencer::{
            conductor::Conductor,
            error::SequencerActorError,
            metrics::{
                update_attributes_build_duration_metrics, update_block_build_duration_metrics,
                update_conductor_commitment_duration_metrics, update_seal_duration_metrics,
                update_total_transactions_sequenced,
            },
            origin_selector::OriginSelector,
        },
    },
};

/// The handle to a block that has been started but not sealed.
#[derive(Debug)]
pub(super) struct UnsealedPayloadHandle {
    /// The [`PayloadId`] of the unsealed payload.
    pub payload_id: PayloadId,
    /// The [`OpAttributesWithParent`] used to start block building.
    pub attributes_with_parent: OpAttributesWithParent,
}

/// The return payload of the `seal_last_and_start_next` function. This allows the sequencer
/// to make an informed decision about when to seal and build the next block.
#[derive(Debug)]
pub(super) struct SealLastStartNextResult {
    /// The [`UnsealedPayloadHandle`] that was built.
    pub unsealed_payload_handle: Option<UnsealedPayloadHandle>,
    /// How long it took to execute the seal operation.
    pub seal_duration: Duration,
}

/// The [`SequencerActor`] is responsible for building L2 blocks on top of the current unsafe head
/// and scheduling them to be signed and gossipped by the P2P layer, extending the L2 chain with new
/// blocks.
#[derive(Debug)]
pub struct SequencerActor<
    AttributesBuilder_,
    Conductor_,
    OriginSelector_,
    SequencerEngineClient_,
    UnsafePayloadGossipClient_,
> where
    AttributesBuilder_: AttributesBuilder,
    Conductor_: Conductor,
    OriginSelector_: OriginSelector,
    SequencerEngineClient_: SequencerEngineClient,
    UnsafePayloadGossipClient_: UnsafePayloadGossipClient,
{
    /// Receiver for admin API requests.
    pub admin_api_rx: mpsc::Receiver<SequencerAdminQuery>,
    /// The attributes builder used for block building.
    pub attributes_builder: AttributesBuilder_,
    /// The cancellation token, shared between all tasks.
    pub cancellation_token: CancellationToken,
    /// The optional conductor RPC client.
    pub conductor: Option<Conductor_>,
    /// The struct used to interact with the engine.
    pub engine_client: SequencerEngineClient_,
    /// Whether the sequencer is active.
    pub is_active: bool,
    /// Whether the sequencer is in recovery mode.
    pub in_recovery_mode: bool,
    /// The struct used to determine the next L1 origin.
    pub origin_selector: OriginSelector_,
    /// The rollup configuration.
    pub rollup_config: Arc<RollupConfig>,
    /// A client to asynchronously sign and gossip built payloads to the network actor.
    pub unsafe_payload_gossip_client: UnsafePayloadGossipClient_,
    /// Backoff to apply when a conductor commit fails. [`None`] disables retry logic entirely.
    pub conductor_commit_backoff: Option<Duration>,
    /// Sealed but NOT YET inserted payload whose conductor commit failed and is pending retry on
    /// the next tick. Only set when `conductor_commit_backoff` is `Some`.
    pub pending_conductor_commit: Option<OpExecutionPayloadEnvelope>,
}

impl<
    AttributesBuilder_,
    Conductor_,
    OriginSelector_,
    SequencerEngineClient_,
    UnsafePayloadGossipClient_,
>
    SequencerActor<
        AttributesBuilder_,
        Conductor_,
        OriginSelector_,
        SequencerEngineClient_,
        UnsafePayloadGossipClient_,
    >
where
    AttributesBuilder_: AttributesBuilder,
    Conductor_: Conductor,
    OriginSelector_: OriginSelector,
    SequencerEngineClient_: SequencerEngineClient,
    UnsafePayloadGossipClient_: UnsafePayloadGossipClient,
{
    /// Seals and commits the last pending block, if one exists and starts the build job for the
    /// next L2 block, on top of the current unsafe head.
    ///
    /// If a new block was started, it will return the associated [`UnsealedPayloadHandle`] so
    /// that it may be sealed and committed in a future call to this function.
    pub(super) async fn seal_last_and_start_next(
        &mut self,
        payload_to_seal: Option<&UnsealedPayloadHandle>,
    ) -> Result<SealLastStartNextResult, SequencerActorError> {
        let seal_duration = match payload_to_seal {
            Some(to_seal) => {
                let seal_start = Instant::now();
                self.seal_and_commit_payload_if_applicable(to_seal).await?;
                seal_start.elapsed()
            }
            None => Duration::default(),
        };

        // Skip starting a new build if a conductor commit is pending retry — the stashed payload
        // will move the unsafe head on the next tick, making any build started now likely to be
        // rejected with `UnsafeHeadChangedSinceBuild`.
        let unsealed_payload_handle = if self.pending_conductor_commit.is_some() {
            None
        } else {
            self.build_unsealed_payload().await?
        };

        Ok(SealLastStartNextResult { unsealed_payload_handle, seal_duration })
    }

    /// Fetches the sealed payload from the engine, commits it to the conductor (if present),
    /// gossips it, and then fires off an insert request to the engine.
    ///
    /// When `conductor_commit_backoff` is set and the conductor commit fails, gossip and insert
    /// are skipped and the payload is stored in `pending_conductor_commit` for retry on the next
    /// tick.
    pub(super) async fn seal_and_commit_payload_if_applicable(
        &mut self,
        unsealed_payload_handle: &UnsealedPayloadHandle,
    ) -> Result<(), SequencerActorError> {
        let seal_request_start = Instant::now();

        // Fetch the sealed payload from the engine WITHOUT inserting it.
        let payload = self
            .engine_client
            .get_sealed_payload(
                unsealed_payload_handle.payload_id,
                unsealed_payload_handle.attributes_with_parent.clone(),
            )
            .await?;

        update_seal_duration_metrics(seal_request_start.elapsed());

        let payload_transaction_count =
            unsealed_payload_handle.attributes_with_parent.count_transactions();
        update_total_transactions_sequenced(payload_transaction_count);

        // If the conductor is available, commit the payload to it BEFORE insertion.
        if let Some(conductor) = &self.conductor {
            let conductor_commitment_start = Instant::now();
            let commit_result = conductor.commit_unsafe_payload(&payload).await;
            update_conductor_commitment_duration_metrics(conductor_commitment_start.elapsed());
            if let Err(err) = commit_result {
                if self.conductor_commit_backoff.is_some() {
                    // Backoff enabled: skip gossip and insert, retain the payload for retry.
                    error!(
                        target: "sequencer",
                        error = %err,
                        "Failed to commit unsafe payload to conductor, will retry on next tick"
                    );
                    self.pending_conductor_commit = Some(payload);
                    return Ok(());
                }
                error!(
                    target: "sequencer",
                    error = %err,
                    "Failed to commit unsafe payload to conductor"
                );
            }
        }

        // Gossip the payload to the p2p network.
        self.unsafe_payload_gossip_client
            .schedule_execution_payload_gossip(payload.clone())
            .await?;

        // Submit the payload to the engine for insertion.
        self.engine_client.insert_unsafe_payload(payload).await?;

        Ok(())
    }

    /// Retries a pending conductor commit stored from a previous failed attempt.
    ///
    /// Returns `Ok(true)` if the commit succeeded (or there was nothing to retry), `Ok(false)` if
    /// it failed again. On success the payload is gossiped and inserted into the engine.
    ///
    /// Gossip or engine-insert failures are propagated as errors so the caller can clear its
    /// in-progress building state and re-anchor on the next tick.
    pub(super) async fn retry_pending_conductor_commit(
        &mut self,
    ) -> Result<bool, SequencerActorError> {
        let Some(ref payload) = self.pending_conductor_commit else {
            return Ok(true);
        };
        let Some(conductor) = &self.conductor else {
            let payload = self.pending_conductor_commit.take().unwrap();
            self.unsafe_payload_gossip_client
                .schedule_execution_payload_gossip(payload.clone())
                .await?;
            self.engine_client.insert_unsafe_payload(payload).await?;
            return Ok(true);
        };
        let conductor_commitment_start = Instant::now();
        match conductor.commit_unsafe_payload(payload).await {
            Ok(()) => {
                update_conductor_commitment_duration_metrics(conductor_commitment_start.elapsed());
                let payload = self.pending_conductor_commit.take().unwrap();
                self.unsafe_payload_gossip_client
                    .schedule_execution_payload_gossip(payload.clone())
                    .await?;
                self.engine_client.insert_unsafe_payload(payload).await?;
                Ok(true)
            }
            Err(err) => {
                update_conductor_commitment_duration_metrics(conductor_commitment_start.elapsed());
                error!(
                    target: "sequencer",
                    error = %err,
                    "Conductor commit retry failed, will retry on next tick"
                );
                Ok(false)
            }
        }
    }

    /// Starts building an L2 block by creating and populating payload attributes referencing the
    /// correct L1 origin block and sending them to the block engine.
    pub(super) async fn build_unsealed_payload(
        &mut self,
    ) -> Result<Option<UnsealedPayloadHandle>, SequencerActorError> {
        let unsafe_head = self.engine_client.get_unsafe_head().await?;

        let Some(l1_origin) = self.get_next_payload_l1_origin(unsafe_head).await? else {
            // Temporary error - retry on next tick.
            return Ok(None);
        };

        info!(
            target: "sequencer",
            parent_num = unsafe_head.block_info.number,
            l1_origin_num = l1_origin.number,
            "Started sequencing new block"
        );

        // Build the payload attributes for the next block.
        let attributes_build_start = Instant::now();

        let Some(attributes_with_parent) = self.build_attributes(unsafe_head, l1_origin).await?
        else {
            // Temporary error or reset - retry on next tick.
            return Ok(None);
        };

        update_attributes_build_duration_metrics(attributes_build_start.elapsed());

        // Send the built attributes to the engine to be built.
        let build_request_start = Instant::now();

        let payload_id =
            self.engine_client.start_build_block(attributes_with_parent.clone()).await?;

        update_block_build_duration_metrics(build_request_start.elapsed());

        Ok(Some(UnsealedPayloadHandle { payload_id, attributes_with_parent }))
    }

    /// Determines and validates the L1 origin block for the provided L2 unsafe head.
    /// Returns `Ok(None)` for temporary errors that should be retried.
    async fn get_next_payload_l1_origin(
        &mut self,
        unsafe_head: L2BlockInfo,
    ) -> Result<Option<BlockInfo>, SequencerActorError> {
        let l1_origin = match self
            .origin_selector
            .next_l1_origin(unsafe_head, self.in_recovery_mode)
            .await
        {
            Ok(l1_origin) => l1_origin,
            Err(err) => {
                warn!(
                    target: "sequencer",
                    ?err,
                    "Temporary error occurred while selecting next L1 origin. Re-attempting on next tick."
                );
                return Ok(None);
            }
        };

        if unsafe_head.l1_origin.hash != l1_origin.parent_hash
            && unsafe_head.l1_origin.hash != l1_origin.hash
        {
            warn!(
                target: "sequencer",
                l1_origin = ?l1_origin,
                unsafe_head_hash = %unsafe_head.l1_origin.hash,
                unsafe_head_l1_origin = ?unsafe_head.l1_origin,
                "Cannot build new L2 block on inconsistent L1 origin, resetting engine"
            );
            self.engine_client.reset_engine_forkchoice().await?;
            return Ok(None);
        }
        Ok(Some(l1_origin))
    }

    /// Builds the `OpAttributesWithParent` for the next block to build. If None is returned, it
    /// indicates that no attributes could be built at this time but future attempts may be made.
    async fn build_attributes(
        &mut self,
        unsafe_head: L2BlockInfo,
        l1_origin: BlockInfo,
    ) -> Result<Option<OpAttributesWithParent>, SequencerActorError> {
        let mut attributes = match self
            .attributes_builder
            .prepare_payload_attributes(unsafe_head, l1_origin.id())
            .await
        {
            Ok(attrs) => attrs,
            Err(PipelineErrorKind::Temporary(_)) => {
                // Temporary error - retry on next tick.
                return Ok(None);
            }
            Err(PipelineErrorKind::Reset(_)) => {
                if let Err(err) = self.engine_client.reset_engine_forkchoice().await {
                    error!(target: "sequencer", ?err, "Failed to reset engine");
                    return Err(SequencerActorError::ChannelClosed);
                }

                warn!(
                    target: "sequencer",
                    "Resetting engine due to pipeline error while preparing payload attributes"
                );
                return Ok(None);
            }
            Err(err @ PipelineErrorKind::Critical(_)) => {
                error!(target: "sequencer", ?err, "Failed to prepare payload attributes");
                return Err(err.into());
            }
        };

        UpgradeActivations::log(&self.rollup_config, &attributes);
        let activator = PoolActivation::new(self.rollup_config.clone());
        attributes.no_tx_pool = Some(!activator.is_enabled(self.in_recovery_mode, l1_origin, &attributes));

        let attrs_with_parent = OpAttributesWithParent::new(attributes, unsafe_head, None, false);
        Ok(Some(attrs_with_parent))
    }

    /// Schedules the initial engine reset request and waits for the unsafe head to be updated.
    async fn schedule_initial_reset(&self) -> Result<(), SequencerActorError> {
        // Reset the engine, in order to initialize the engine state.
        // NB: this call waits for confirmation that the reset succeeded and we can proceed with
        // post-reset logic.
        self.engine_client.reset_engine_forkchoice().await.map_err(|err| {
            error!(target: "sequencer", ?err, "Failed to send reset request to engine");
            err.into()
        })
    }
}

#[async_trait]
impl<
    AttributesBuilder_,
    Conductor_,
    OriginSelector_,
    SequencerEngineClient_,
    UnsafePayloadGossipClient_,
> NodeActor
    for SequencerActor<
        AttributesBuilder_,
        Conductor_,
        OriginSelector_,
        SequencerEngineClient_,
        UnsafePayloadGossipClient_,
    >
where
    AttributesBuilder_: AttributesBuilder + Sync + 'static,
    Conductor_: Conductor + Sync + 'static,
    OriginSelector_: OriginSelector + Sync + 'static,
    SequencerEngineClient_: SequencerEngineClient + Sync + 'static,
    UnsafePayloadGossipClient_: UnsafePayloadGossipClient + Sync + 'static,
{
    type Error = SequencerActorError;
    type StartData = ();

    async fn start(mut self, _: Self::StartData) -> Result<(), Self::Error> {
        let mut build_ticker =
            tokio::time::interval(Duration::from_secs(self.rollup_config.block_time));

        self.update_metrics();

        // Reset the engine state prior to beginning block building.
        self.schedule_initial_reset().await?;

        let mut next_payload_to_seal: Option<UnsealedPayloadHandle> = None;
        let mut last_seal_duration = Duration::from_secs(0);
        loop {
            select! {
                // We are using a biased select here to ensure that the admin queries are given priority over the block building task.
                // This is important to limit the occurrence of race conditions where a stopped query is received when a sequencer is building a new block.
                biased;
                _ = self.cancellation_token.cancelled() => {
                    info!(
                        target: "sequencer",
                        "Received shutdown signal. Exiting sequencer task."
                    );
                    return Ok(());
                }
                Some(query) = self.admin_api_rx.recv() => {
                    let active_before = self.is_active;

                    self.handle_admin_query(query).await;

                    // immediately attempt to build a block if the sequencer was just started
                    if !active_before && self.is_active {
                        build_ticker.reset_immediately();
                    }
                }
                // The sequencer must be active to build new blocks.
                _ = build_ticker.tick(), if self.is_active => {

                    // If a previous conductor commit is pending, retry it before doing anything
                    // else. On failure, back off and skip this tick entirely.
                    if let Some(backoff) = self.conductor_commit_backoff
                        && self.pending_conductor_commit.is_some()
                    {
                        match self.retry_pending_conductor_commit().await {
                            Ok(true) => {}
                            Ok(false) => {
                                build_ticker.reset_after(backoff);
                                continue;
                            }
                            Err(err) => {
                                warn!(target: "sequencer", error = ?err, "Gossip or insert failed during conductor commit retry");
                                next_payload_to_seal = None;
                                continue;
                            }
                        }
                    }

                    match self.seal_last_and_start_next(next_payload_to_seal.as_ref()).await {
                        Ok(res) => {
                            next_payload_to_seal = res.unsealed_payload_handle;
                            last_seal_duration = res.seal_duration;
                        },
                        Err(SequencerActorError::EngineError(EngineClientError::SealError(err))) => {
                            if is_seal_task_err_fatal(&err) {
                                error!(target: "sequencer", error = ?err, "Critical seal task error occurred");
                                self.cancellation_token.cancel();
                                return Err(SequencerActorError::EngineError(EngineClientError::SealError(err)));
                            }
                            next_payload_to_seal = None;
                        },
                        Err(SequencerActorError::PayloadGossip(err)) => {
                            warn!(target: "sequencer", error = ?err, "Failed to gossip payload");
                            next_payload_to_seal = None;
                        },
                        Err(SequencerActorError::EngineError(
                            err @ EngineClientError::RequestError(_),
                        )) => {
                            warn!(target: "sequencer", error = ?err, "Failed to insert unsafe payload into engine");
                            next_payload_to_seal = None;
                        },
                        Err(other_err) => {
                            error!(target: "sequencer", error = ?other_err, "Unexpected error building or sealing payload");
                            self.cancellation_token.cancel();
                            return Err(other_err);
                        }
                    }

                    // If a fresh conductor commit failure just occurred, back off before
                    // attempting to seal or build the next block.
                    if let Some(backoff) = self.conductor_commit_backoff
                        && self.pending_conductor_commit.is_some()
                    {
                        build_ticker.reset_after(backoff);
                        continue;
                    }

                    if let Some(ref payload) = next_payload_to_seal {
                        let next_block_seconds = payload.attributes_with_parent.parent().block_info.timestamp.saturating_add(self.rollup_config.block_time);
                        // next block time is last + block_time - time it takes to seal.
                        let next_block_time = UNIX_EPOCH + Duration::from_secs(next_block_seconds) - last_seal_duration;
                        match next_block_time.duration_since(SystemTime::now()) {
                            Ok(duration) => build_ticker.reset_after(duration),
                            Err(_) => build_ticker.reset_immediately(),
                        };
                    } else {
                        build_ticker.reset_immediately();
                    }
                }
            }
        }
    }
}

impl<
    AttributesBuilder_,
    Conductor_,
    OriginSelector_,
    SequencerEngineClient_,
    UnsafePayloadGossipClient_,
> CancellableContext
    for SequencerActor<
        AttributesBuilder_,
        Conductor_,
        OriginSelector_,
        SequencerEngineClient_,
        UnsafePayloadGossipClient_,
    >
where
    AttributesBuilder_: AttributesBuilder,
    Conductor_: Conductor,
    OriginSelector_: OriginSelector,
    SequencerEngineClient_: SequencerEngineClient,
    UnsafePayloadGossipClient_: UnsafePayloadGossipClient,
{
    fn cancelled(&self) -> WaitForCancellationFuture<'_> {
        self.cancellation_token.cancelled()
    }
}

// Determines whether the provided [`SealTaskError`] is fatal for the sequencer.
//
// NB: We could use `err.severity()`, but that gives EngineActor control over this classification.
// `SequencerActor` may have different interpretations of severity, and it is not clear when making
// a change in that area of the codebase that it will affect this area. When a new task error is
// added, this approach guarantees compilation will fail until it is handled here.
fn is_seal_task_err_fatal(err: &SealTaskError) -> bool {
    match err {
        SealTaskError::PayloadInsertionFailed(insert_err) => match &**insert_err {
            InsertTaskError::ForkchoiceUpdateFailed(synchronize_error) => match synchronize_error {
                SynchronizeTaskError::FinalizedAheadOfUnsafe(_, _) => true,
                SynchronizeTaskError::ForkchoiceUpdateFailed(_)
                | SynchronizeTaskError::InvalidForkchoiceState
                | SynchronizeTaskError::UnexpectedPayloadStatus(_) => false,
            },
            InsertTaskError::FromBlockError(_) | InsertTaskError::L2BlockInfoConstruction(_) => {
                true
            }
            InsertTaskError::InsertFailed(_) | InsertTaskError::UnexpectedPayloadStatus(_) => false,
        },
        SealTaskError::GetPayloadFailed(_)
        | SealTaskError::HoloceneInvalidFlush
        | SealTaskError::UnsafeHeadChangedSinceBuild => false,
        SealTaskError::DepositOnlyPayloadFailed
        | SealTaskError::DepositOnlyPayloadReattemptFailed
        | SealTaskError::FromBlock(_)
        | SealTaskError::MpscSend(_)
        | SealTaskError::ClockWentBackwards => true,
    }
}
