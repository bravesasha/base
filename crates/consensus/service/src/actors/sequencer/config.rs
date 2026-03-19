//! Configuration for the [`SequencerActor`].
//!
//! [`SequencerActor`]: super::SequencerActor

use std::time::Duration;

use url::Url;

/// Configuration for the [`SequencerActor`].
///
/// [`SequencerActor`]: super::SequencerActor
#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct SequencerConfig {
    /// Whether or not the sequencer is enabled at startup.
    pub sequencer_stopped: bool,
    /// Whether or not the sequencer is in recovery mode.
    pub sequencer_recovery_mode: bool,
    /// The [`Url`] for the conductor RPC endpoint. If [`Some`], enables the conductor service.
    pub conductor_rpc_url: Option<Url>,
    /// The confirmation delay for the sequencer.
    pub l1_conf_delay: u64,
    /// Backoff duration to apply when a conductor commit fails. When [`Some`], the sequencer
    /// will skip gossip on failure, retain the payload, and retry the commit after this delay.
    /// When [`None`] (the default), conductor commit failures are logged and ignored.
    pub conductor_commit_backoff: Option<Duration>,
}
