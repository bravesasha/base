#![allow(missing_docs)]

use alloy_eips::eip7910::EthConfig;
use alloy_provider::Provider;
use base_flashblocks_node::test_harness::FlashblocksHarness;
use eyre::Result;

fn assert_zero_blob_schedule(config: &EthConfig) {
    let current = config.current.blob_schedule;
    assert_eq!(current.update_fraction, 0);
    assert_eq!(current.max_blob_count, 0);
    assert_eq!(current.target_blob_count, 0);
}

#[tokio::test]
async fn eth_config_remains_available_with_flashblocks_extension() -> Result<()> {
    let harness = FlashblocksHarness::new().await?;
    let provider = harness.provider();

    let config = provider.client().request_noparams::<EthConfig>("eth_config").await?;
    assert_zero_blob_schedule(&config);

    Ok(())
}
