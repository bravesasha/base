use std::time::Duration;

use alloy_primitives::B256;
use alloy_rpc_types_engine::ExecutionPayloadV1;
use alloy_transport::RpcError;
use base_alloy_rpc_types_engine::{
    OpExecutionPayload, OpExecutionPayloadEnvelope, OpPayloadAttributes,
};
use base_consensus_derive::{BuilderError, PipelineErrorKind, test_utils::TestAttributesBuilder};
use base_protocol::{BlockInfo, L2BlockInfo, OpAttributesWithParent};
use rstest::rstest;

#[cfg(test)]
use crate::{
    ConductorError, SequencerActorError,
    actors::{
        MockConductor, MockOriginSelector, MockSequencerEngineClient,
        MockUnsafePayloadGossipClient,
        sequencer::{actor::UnsealedPayloadHandle, tests::test_util::test_actor},
    },
};

fn dummy_envelope() -> OpExecutionPayloadEnvelope {
    OpExecutionPayloadEnvelope {
        parent_beacon_block_root: None,
        execution_payload: OpExecutionPayload::V1(ExecutionPayloadV1 {
            parent_hash: B256::ZERO,
            fee_recipient: alloy_primitives::Address::ZERO,
            state_root: B256::ZERO,
            receipts_root: B256::ZERO,
            logs_bloom: alloy_primitives::Bloom::ZERO,
            prev_randao: B256::ZERO,
            block_number: 1,
            gas_limit: 0,
            gas_used: 0,
            timestamp: 0,
            extra_data: alloy_primitives::Bytes::new(),
            base_fee_per_gas: alloy_primitives::U256::ZERO,
            block_hash: B256::ZERO,
            transactions: vec![],
        }),
    }
}

fn conductor_rpc_error() -> ConductorError {
    ConductorError::Rpc(RpcError::local_usage_str("test conductor error"))
}

fn dummy_attributes_with_parent() -> OpAttributesWithParent {
    OpAttributesWithParent::new(OpPayloadAttributes::default(), L2BlockInfo::default(), None, false)
}

#[rstest]
#[case::temp(PipelineErrorKind::Temporary(BuilderError::Custom(String::new()).into()), false)]
#[case::reset(PipelineErrorKind::Reset(BuilderError::Custom(String::new()).into()), false)]
#[case::critical(PipelineErrorKind::Critical(BuilderError::Custom(String::new()).into()), true)]
#[tokio::test]
async fn test_build_unsealed_payload_prepare_payload_attributes_error(
    #[case] forced_error: PipelineErrorKind,
    #[case] expect_err: bool,
) {
    let mut client = MockSequencerEngineClient::new();

    let unsafe_head = L2BlockInfo::default();
    client.expect_get_unsafe_head().times(1).return_once(move || Ok(unsafe_head));
    // Must not be called on critical error
    client.expect_start_build_block().times(0);
    if let PipelineErrorKind::Reset(_) = &forced_error {
        client.expect_reset_engine_forkchoice().times(1).return_once(move || Ok(()));
    }

    let l1_origin = BlockInfo::default();
    let mut origin_selector = MockOriginSelector::new();
    origin_selector.expect_next_l1_origin().times(1).return_once(move |_, _| Ok(l1_origin));

    let attributes_builder = TestAttributesBuilder { attributes: vec![Err(forced_error)] };

    let mut actor = test_actor();
    actor.origin_selector = origin_selector;
    actor.engine_client = client;
    actor.attributes_builder = attributes_builder;

    let result = actor.build_unsealed_payload().await;
    if expect_err {
        assert!(result.is_err());
        assert!(matches!(
            result.unwrap_err(),
            SequencerActorError::AttributesBuilder(PipelineErrorKind::Critical(_))
        ));
    } else {
        assert!(result.is_ok());
    }
}

// --- get_sealed_payload failure test ---

#[tokio::test]
async fn test_seal_and_commit_get_sealed_payload_failure_propagates() {
    use crate::actors::engine::EngineClientError;

    // When get_sealed_payload fails, the error must propagate immediately.
    // No conductor commit, gossip, or insert should be attempted.
    let mut client = MockSequencerEngineClient::new();
    client
        .expect_get_sealed_payload()
        .times(1)
        .return_once(|_, _| Err(EngineClientError::RequestError("engine offline".to_string())));
    client.expect_insert_unsafe_payload().times(0);

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(0);

    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(0);

    let mut actor = test_actor();
    actor.engine_client = client;
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;

    let handle = UnsealedPayloadHandle {
        payload_id: Default::default(),
        attributes_with_parent: dummy_attributes_with_parent(),
    };
    let result = actor.seal_and_commit_payload_if_applicable(&handle).await;

    assert!(result.is_err());
}

// --- conductor commit tests ---

#[tokio::test]
async fn test_conductor_commit_failure_skips_gossip_when_backoff_enabled() {
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Err(conductor_rpc_error()));

    // Gossip must NOT be called when the commit fails and backoff is enabled.
    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(0);

    let mut client = MockSequencerEngineClient::new();
    client.expect_get_sealed_payload().times(1).return_once(move |_, _| Ok(envelope));
    // Insert must NOT be called when the conductor commit fails with backoff enabled.
    client.expect_insert_unsafe_payload().times(0);

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;
    actor.engine_client = client;
    actor.conductor_commit_backoff = Some(Duration::from_millis(1000));

    let handle = UnsealedPayloadHandle {
        payload_id: Default::default(),
        attributes_with_parent: dummy_attributes_with_parent(),
    };
    let result = actor.seal_and_commit_payload_if_applicable(&handle).await;

    assert!(result.is_ok());
    assert!(actor.pending_conductor_commit.is_some());
}

#[tokio::test]
async fn test_conductor_commit_failure_gossips_when_backoff_disabled() {
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Err(conductor_rpc_error()));

    // Gossip MUST still fire when backoff is disabled (existing behaviour).
    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(1).return_once(|_| Ok(()));

    let mut client = MockSequencerEngineClient::new();
    client.expect_get_sealed_payload().times(1).return_once(move |_, _| Ok(envelope));
    // Insert MUST still be called when backoff is disabled (existing behaviour).
    client.expect_insert_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;
    actor.engine_client = client;
    // conductor_commit_backoff is None by default in test_actor()

    let handle = UnsealedPayloadHandle {
        payload_id: Default::default(),
        attributes_with_parent: dummy_attributes_with_parent(),
    };
    let result = actor.seal_and_commit_payload_if_applicable(&handle).await;

    assert!(result.is_ok());
    assert!(actor.pending_conductor_commit.is_none());
}

#[tokio::test]
async fn test_conductor_commit_success_gossips_and_clears_pending() {
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(1).return_once(|_| Ok(()));

    let mut client = MockSequencerEngineClient::new();
    client.expect_get_sealed_payload().times(1).return_once(move |_, _| Ok(envelope));
    // Insert must be called on conductor commit success.
    client.expect_insert_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;
    actor.engine_client = client;
    actor.conductor_commit_backoff = Some(Duration::from_millis(1000));

    let handle = UnsealedPayloadHandle {
        payload_id: Default::default(),
        attributes_with_parent: dummy_attributes_with_parent(),
    };
    let result = actor.seal_and_commit_payload_if_applicable(&handle).await;

    assert!(result.is_ok());
    assert!(actor.pending_conductor_commit.is_none());
}

#[tokio::test]
async fn test_retry_pending_conductor_commit_success_gossips_and_clears() {
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(1).return_once(|_| Ok(()));

    let mut client = MockSequencerEngineClient::new();
    // Insert must be called after successful conductor commit retry.
    client.expect_insert_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;
    actor.engine_client = client;
    actor.pending_conductor_commit = Some(envelope);

    let succeeded = actor.retry_pending_conductor_commit().await;

    assert!(succeeded);
    assert!(actor.pending_conductor_commit.is_none());
}

#[tokio::test]
async fn test_retry_pending_conductor_commit_failure_skips_gossip_and_preserves_pending() {
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Err(conductor_rpc_error()));

    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(0);

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.unsafe_payload_gossip_client = gossip;
    actor.pending_conductor_commit = Some(envelope);

    let succeeded = actor.retry_pending_conductor_commit().await;

    assert!(!succeeded);
    assert!(actor.pending_conductor_commit.is_some());
}

#[tokio::test]
async fn test_retry_pending_no_conductor_gossips_and_inserts() {
    let envelope = dummy_envelope();

    // With no conductor, the pending payload must still be gossiped and inserted so the
    // block is not silently dropped.
    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(1).return_once(|_| Ok(()));

    let mut client = MockSequencerEngineClient::new();
    client.expect_insert_unsafe_payload().times(1).return_once(|_| Ok(()));

    let mut actor = test_actor();
    actor.conductor = None;
    actor.unsafe_payload_gossip_client = gossip;
    actor.engine_client = client;
    actor.pending_conductor_commit = Some(envelope);

    let succeeded = actor.retry_pending_conductor_commit().await;

    assert!(succeeded);
    assert!(actor.pending_conductor_commit.is_none());
}

// --- seal_last_and_start_next skips build on pending conductor commit ---

#[tokio::test]
async fn test_seal_last_and_start_next_skips_build_when_conductor_commit_stashed() {
    // When a conductor commit fails (with backoff enabled) and the payload is stashed,
    // `seal_last_and_start_next` must NOT call `build_unsealed_payload()` — doing so
    // would kick off a wasted EL block build that will be rejected on the next tick once
    // the stashed payload is inserted and the unsafe head moves.
    let envelope = dummy_envelope();

    let mut conductor = MockConductor::new();
    conductor.expect_commit_unsafe_payload().times(1).return_once(|_| Err(conductor_rpc_error()));

    let mut client = MockSequencerEngineClient::new();
    client.expect_get_sealed_payload().times(1).return_once(move |_, _| Ok(envelope));
    // The stashed payload is not inserted immediately.
    client.expect_insert_unsafe_payload().times(0);
    // build_unsealed_payload must not be called — these are its two engine RPCs.
    client.expect_get_unsafe_head().times(0);
    client.expect_start_build_block().times(0);

    let mut gossip = MockUnsafePayloadGossipClient::new();
    gossip.expect_schedule_execution_payload_gossip().times(0);

    let mut actor = test_actor();
    actor.conductor = Some(conductor);
    actor.engine_client = client;
    actor.unsafe_payload_gossip_client = gossip;
    actor.conductor_commit_backoff = Some(Duration::from_millis(1000));

    let handle = UnsealedPayloadHandle {
        payload_id: Default::default(),
        attributes_with_parent: dummy_attributes_with_parent(),
    };
    let result = actor.seal_last_and_start_next(Some(&handle)).await;

    assert!(result.is_ok());
    // No new build was started.
    assert!(result.unwrap().unsealed_payload_handle.is_none());
    // The payload is held for retry on the next tick.
    assert!(actor.pending_conductor_commit.is_some());
}
