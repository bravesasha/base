//! Integration tests that replace the legacy Base V1 shell smoke checks with
//! RPC-visible behavior exercised against local nodes.

use std::sync::Arc;

use alloy_eips::eip7910::EthConfig;
use alloy_primitives::{Address, Bytes, U256, address, bytes};
use alloy_provider::{Provider, RootProvider};
use alloy_rpc_types_eth::{TransactionInput, state::AccountOverride};
use base_alloy_network::Base;
use base_alloy_rpc_types::OpTransactionRequest;
use base_execution_chainspec::OpChainSpec;
use base_node_runner::test_utils::{
    TestHarness, TestHarnessBuilder, build_test_genesis, build_test_genesis_v1,
};
use eyre::Result;

const CLZ_PROBE_ADDRESS: Address = address!("0x000000000000000000000000000000000000001e");
const CLZ_RUNTIME: Bytes = bytes!("0x6000351e60005260206000f3");

const MODEXP_ADDRESS: Address = address!("0x0000000000000000000000000000000000000005");
const MODEXP_OVERSIZED_INPUT: Bytes = bytes!(
    "0x000000000000000000000000000000000000000000000000000000000000040100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001"
);

const MODEXP_GAS_PROBE_ADDRESS: Address = address!("0x000000000000000000000000000000000000001d");
const MODEXP_GAS_PROBE_RUNTIME: Bytes = bytes!("0x600060006060600060006005610190f160005260206000f3");

const P256_GAS_PROBE_ADDRESS: Address = address!("0x000000000000000000000000000000000000001f");
const P256_GAS_PROBE_RUNTIME: Bytes = bytes!("0x60006000600060006000610100611388f160005260206000f3");

async fn build_pre_v1_harness() -> Result<TestHarness> {
    let chain_spec = Arc::new(OpChainSpec::from_genesis(build_test_genesis()));
    TestHarnessBuilder::new().with_chain_spec(chain_spec).build().await
}

async fn build_v1_harness() -> Result<TestHarness> {
    let chain_spec = Arc::new(OpChainSpec::from_genesis(build_test_genesis_v1()));
    TestHarnessBuilder::new().with_chain_spec(chain_spec).build().await
}

fn assert_zero_blob_schedule(config: &EthConfig) {
    let current = config.current.blob_schedule;
    assert_eq!(current.update_fraction, 0);
    assert_eq!(current.max_blob_count, 0);
    assert_eq!(current.target_blob_count, 0);

    if let Some(next) = config.next.as_ref() {
        assert_eq!(next.blob_schedule.update_fraction, 0);
        assert_eq!(next.blob_schedule.max_blob_count, 0);
        assert_eq!(next.blob_schedule.target_blob_count, 0);
    }

    if let Some(last) = config.last.as_ref() {
        assert_eq!(last.blob_schedule.update_fraction, 0);
        assert_eq!(last.blob_schedule.max_blob_count, 0);
        assert_eq!(last.blob_schedule.target_blob_count, 0);
    }
}

fn call_request(to: Address, input: Bytes, gas_limit: u64) -> OpTransactionRequest {
    OpTransactionRequest::default()
        .to(to)
        .gas_limit(gas_limit)
        .input(TransactionInput::new(input))
}

fn decode_word(output: &Bytes) -> U256 {
    U256::from_be_slice(output.as_ref())
}

async fn assert_eth_config_available(provider: &RootProvider<Base>) -> Result<()> {
    let config = provider.client().request_noparams::<EthConfig>("eth_config").await?;
    assert_zero_blob_schedule(&config);
    Ok(())
}

async fn call_clz(provider: &RootProvider<Base>, input: Bytes) -> Result<Bytes> {
    Ok(provider
        .call(call_request(CLZ_PROBE_ADDRESS, input, 100_000))
        .latest()
        .account_override(CLZ_PROBE_ADDRESS, AccountOverride::default().with_code(CLZ_RUNTIME))
        .await?)
}

async fn call_modexp_oversized(provider: &RootProvider<Base>) -> Result<Bytes> {
    Ok(provider
        .call(call_request(MODEXP_ADDRESS, MODEXP_OVERSIZED_INPUT, 1_000_000))
        .latest()
        .await?)
}

async fn call_with_probe_runtime(
    provider: &RootProvider<Base>,
    probe_address: Address,
    runtime: Bytes,
) -> Result<Bytes> {
    Ok(provider
        .call(call_request(probe_address, Bytes::default(), 100_000))
        .latest()
        .account_override(probe_address, AccountOverride::default().with_code(runtime))
        .await?)
}

#[tokio::test]
async fn pre_v1_node_exposes_pre_osaka_rpc_behavior() -> Result<()> {
    let harness = build_pre_v1_harness().await?;
    let provider = harness.provider();

    let clz_err = call_clz(
        &provider,
        bytes!("0x0000000000000000000000000000000000000000000000000000000000000001"),
    )
    .await;
    assert!(clz_err.is_err(), "CLZ must be unavailable before Base V1");

    let oversized = call_modexp_oversized(&provider).await;
    assert!(oversized.is_ok(), "oversized MODEXP input must still be accepted before Base V1");

    let modexp = call_with_probe_runtime(
        &provider,
        MODEXP_GAS_PROBE_ADDRESS,
        MODEXP_GAS_PROBE_RUNTIME,
    )
    .await?;
    assert_eq!(
        decode_word(&modexp),
        U256::from(1),
        "MODEXP probe with 400 gas must succeed before Base V1"
    );

    let p256 =
        call_with_probe_runtime(&provider, P256_GAS_PROBE_ADDRESS, P256_GAS_PROBE_RUNTIME).await?;
    assert_eq!(
        decode_word(&p256),
        U256::from(1),
        "P256 probe with 5000 gas must succeed before Base V1"
    );

    assert_eth_config_available(&provider).await?;

    Ok(())
}

#[tokio::test]
async fn base_v1_node_exposes_osaka_rpc_behavior() -> Result<()> {
    let harness = build_v1_harness().await?;
    let provider = harness.provider();

    for (label, input, expected) in [
        (
            "zero",
            bytes!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            U256::from(256),
        ),
        (
            "one",
            bytes!("0x0000000000000000000000000000000000000000000000000000000000000001"),
            U256::from(255),
        ),
        (
            "high-bit",
            bytes!("0x8000000000000000000000000000000000000000000000000000000000000000"),
            U256::ZERO,
        ),
        (
            "four-bits",
            bytes!("0x0f00000000000000000000000000000000000000000000000000000000000000"),
            U256::from(4),
        ),
    ] {
        let output = call_clz(&provider, input).await?;
        assert_eq!(decode_word(&output), expected, "unexpected CLZ result for {label}");
    }

    let oversized = call_modexp_oversized(&provider).await;
    assert!(oversized.is_err(), "oversized MODEXP input must be rejected after Base V1");

    let modexp = call_with_probe_runtime(
        &provider,
        MODEXP_GAS_PROBE_ADDRESS,
        MODEXP_GAS_PROBE_RUNTIME,
    )
    .await?;
    assert_eq!(
        decode_word(&modexp),
        U256::ZERO,
        "MODEXP probe with 400 gas must fail after Base V1"
    );

    let p256 =
        call_with_probe_runtime(&provider, P256_GAS_PROBE_ADDRESS, P256_GAS_PROBE_RUNTIME).await?;
    assert_eq!(
        decode_word(&p256),
        U256::ZERO,
        "P256 probe with 5000 gas must fail after Base V1"
    );

    assert_eth_config_available(&provider).await?;

    Ok(())
}
