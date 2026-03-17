//! Chain specification for the Base Zeronet network.

use alloc::sync::Arc;

use reth_primitives_traits::sync::LazyLock;

use crate::OpChainSpec;

/// The Base Zeronet spec
///
/// Chain ID: 763360 (0xBA5E0 — "BASE0" in leet hex, i.e. "base zero")
///
/// All hardforks from Bedrock through Jovian are activated at genesis (timestamp 0).
/// Base V1 is excluded (absent from genesis config = never activated).
///
/// Fork activation times are specified in the genesis JSON rather than as a
/// compile-time hardfork constant. The [`OpChainSpec`] `From<Genesis>` impl reads
/// these fields and builds the hardfork schedule automatically.
///
/// NOTE: The genesis file is a placeholder. Replace with the output of
/// `genesis/generate-l2-genesis.sh base-zeronet` once L1 contracts are deployed.
/// After replacing the genesis, update the chain ID in `deploy-config.json` to
/// match and re-run genesis generation.
pub static BASE_ZERONET: LazyLock<Arc<OpChainSpec>> = LazyLock::new(|| {
    let genesis =
        serde_json::from_str(include_str!("../res/genesis/zeronet_sepolia_base.json"))
            .expect("Can't deserialize Base Zeronet genesis json");
    Arc::new(OpChainSpec::from_genesis(genesis))
});
