//! Contains a handle wrapper type to an unsealed payload to allow for iterative retries.

use base_protocol::OpAttributesWithParent;

/// The `SealStatus` enum represents the status of a sealing operation.
#[derive(Debug, Clone, PartialEq)]
pub enum SealStatus {
    /// The sealing process hasn't started yet.
    NotStarted,
    /// The sealing process has committed the payload to conductor.
    Committed,
    /// The sealing process has gossiped the payload to the network.
    Gossiped,
    /// The sealing process has inserted the payload and is complete.
    Inserted,
}

impl SealStatus {
    /// Returns if the sealing process is complete, which is when the status is `Inserted`.
    pub fn is_complete(&self) -> bool {
        matches!(self, SealStatus::Inserted)
    }

    /// Returns if the sealing process has started, which is when the status is not `NotStarted`.
    pub fn has_started(&self) -> bool {
        !matches!(self, SealStatus::NotStarted)
    }
}

/// The `SealError` enum represents errors that can occur during the sealing process.
#[derive(Debug, thiserror::Error)]
pub enum SealError {
    /// The sealing process failed to finish in time for a newly built block.
    #[error("sealing process failed to finish in time for newly built block")]
    CriticalUnfinishedSealed,
}

/// The `PayloadSealer` struct is a wrapper around an unsealed payload that tracks the sealing status.
#[derive(Debug, Clone)]
pub struct PayloadSealer {
    /// The unsealed payload that is being tracked.
    pub current_payload: OpAttributesWithParent,
    /// The current status of the sealing process for the payload.
    pub seal_status: SealStatus,
    /// Whether a job is currently in flight.
    pub in_flight: bool,
}

impl PayloadSealer {
    /// Attempts to make progress on the payload sealer if required.
    pub fn step(&mut self, payload: Option<OpAttributesWithParent>) -> Result<(), SealError> {
        if let Some(attributes) = payload {
            if !self.seal_status.is_complete() && attributes != self.current_payload {
                error!(target: "sequencer", "Critical error: sealing process failed to finish in time for newly built block");
                // When this is returned, it is expected that the call site handle this.
                // Some options could be to transfer leadership, set a metric, etc.
                return Err(SealError::CriticalUnfinishedSealed);
            }

            self.reset(attributes);
        }

        self.inner_step();

        Ok(())
    }

    /// Performs the inner step.
    pub fn inner_step(&mut self) {
        if self.in_flight {
            trace!(target: "sequencer", "sealing is inflight, skipping");
            return;
        }

        match self.seal_status {
            SealStatus::NotStarted => {
                trace!(target: "sequencer", "starting sealing process");
                self.in_flight = true;
                // TODO: spawn an async task to perform the commit to conductor, and upon completion, update the seal status to `Committed`.
            }
            SealStatus::Committed => {
                trace!(target: "sequencer", "committed payload, gossiping to network");
                self.in_flight = true;
                // TODO: spawn an async task to perform the gossip to the network, and upon completion, update the seal status to `Gossiped`.
            }
            SealStatus::Gossiped => {
                trace!(target: "sequencer", "gossiped payload, inserting into local engine");
                self.in_flight = true;
                // TODO: spawn an async task to perform the engine client insert, and upon completion, update the seal status to `Inserted`.
            }
            SealStatus::Inserted => {
                trace!(target: "sequencer", "payload inserted, sealing process complete");
            }
        }
    }

    /// Resets the [`PayloadSealer`] with the new provided payload, and resets the sealing status to `NotStarted`.
    pub fn reset(&mut self, payload: OpAttributesWithParent) {
        self.current_payload = payload;
        self.seal_status = SealStatus::NotStarted;
    }
}


