//! Base-specific `eth_config` RPC support.

use alloy_eips::{
    eip4844::BLOB_TX_MIN_BLOB_GASPRICE,
    eip7840::BlobParams,
    eip7910::{EthConfig, EthForkConfig},
};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_chainspec::{ChainSpecProvider, EthereumHardforks, Hardforks};
use reth_evm::ConfigureEvm;
use reth_node_api::NodePrimitives;
use reth_primitives_traits::header::HeaderMut;
use reth_rpc_eth_api::helpers::config::{EthConfigApiServer, EthConfigHandler};
use reth_storage_api::BlockReaderIdExt;

/// RPC endpoint support for Base's `eth_config` response.
///
/// Base does not currently support native blob transactions, so its `blobSchedule` should be
/// reported as zeroed rather than inheriting synthetic Cancun/Prague/Osaka defaults from the
/// upstream Ethereum handler.
#[cfg_attr(not(feature = "client"), rpc(server, namespace = "eth"))]
#[cfg_attr(feature = "client", rpc(server, client, namespace = "eth"))]
pub trait BaseEthConfigApi {
    /// Returns an object with data about recent and upcoming fork configurations.
    #[method(name = "config")]
    fn config(&self) -> RpcResult<EthConfig>;
}

/// Base-specific handler for the `eth_config` RPC endpoint.
#[derive(Debug, Clone)]
pub struct BaseEthConfigHandler<Provider, Evm> {
    provider: Provider,
    evm_config: Evm,
}

impl<Provider, Evm> BaseEthConfigHandler<Provider, Evm> {
    /// Creates a new [`BaseEthConfigHandler`].
    pub const fn new(provider: Provider, evm_config: Evm) -> Self {
        Self { provider, evm_config }
    }
}

impl<Provider, Evm> BaseEthConfigHandler<Provider, Evm>
where
    Provider: ChainSpecProvider<ChainSpec: Hardforks + EthereumHardforks>
        + BlockReaderIdExt<Header: HeaderMut>
        + Clone
        + 'static,
    Evm: ConfigureEvm<Primitives: NodePrimitives<BlockHeader = Provider::Header>> + Clone + 'static,
{
    const fn zero_blob_params() -> BlobParams {
        BlobParams {
            target_blob_count: 0,
            max_blob_count: 0,
            update_fraction: 0,
            min_blob_fee: BLOB_TX_MIN_BLOB_GASPRICE,
            max_blobs_per_tx: 0,
            blob_base_cost: 0,
        }
    }

    fn zero_blob_schedule(fork_config: &mut EthForkConfig) {
        fork_config.blob_schedule = Self::zero_blob_params();
    }

    fn sanitize_blob_schedules(&self, config: &mut EthConfig) {
        Self::zero_blob_schedule(&mut config.current);

        if let Some(next) = config.next.as_mut() {
            Self::zero_blob_schedule(next);
        }

        if let Some(last) = config.last.as_mut() {
            Self::zero_blob_schedule(last);
        }
    }
}

impl<Provider, Evm> BaseEthConfigApiServer for BaseEthConfigHandler<Provider, Evm>
where
    Provider: ChainSpecProvider<ChainSpec: Hardforks + EthereumHardforks>
        + BlockReaderIdExt<Header: HeaderMut>
        + Clone
        + 'static,
    Evm: ConfigureEvm<Primitives: NodePrimitives<BlockHeader = Provider::Header>> + Clone + 'static,
{
    fn config(&self) -> RpcResult<EthConfig> {
        let mut config = EthConfigApiServer::config(&EthConfigHandler::new(
            self.provider.clone(),
            self.evm_config.clone(),
        ))?;
        self.sanitize_blob_schedules(&mut config);
        Ok(config)
    }
}
