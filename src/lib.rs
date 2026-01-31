use crate::yellow_util::map_commitment_level;
use crate::LooperError::{CannotRequestSlots, OnlyCommittedProcessedAllowed};
use solana_commitment_config::CommitmentConfig;
use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots;
use yellowstone_grpc_proto::prelude::SubscribeRequest;
use yellowstone_grpc_proto::prost;

pub mod geyser_looper;
mod yellow_util;

#[derive(Clone, Debug, PartialEq)]
pub struct LooperSubscribeRequest {
    inner: SubscribeRequest,
}

#[derive(Debug, thiserror::Error)]
pub enum LooperError {
    #[error("LooperSubscribeRequest does not support user-defined slot subscriptions; they will be ignored")]
    CannotRequestSlots,

    #[error("LooperSubscribeRequest only supports CommitmentConfig::processed()")]
    OnlyCommittedProcessedAllowed,
}

impl TryFrom<SubscribeRequest> for LooperSubscribeRequest {
    type Error = LooperError;

    fn try_from(subscription: SubscribeRequest) -> Result<Self, Self::Error> {
        if !subscription.slots.is_empty() {
            return Err(CannotRequestSlots);
        }
        // force callers to set processed to avoid confusion
        if subscription.commitment
            != Some(map_commitment_level(CommitmentConfig::processed()) as i32)
        {
            return Err(OnlyCommittedProcessedAllowed);
        }

        let magic_slots_subscription = SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        };

        let subscribe_request = SubscribeRequest {
            slots: HashMap::from([(
                "_magic_confirmed_slots".to_string(),
                magic_slots_subscription,
            )]),
            commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
            ..subscription
        };

        Ok(Self {
            inner: subscribe_request,
        })
    }
}

impl From<LooperSubscribeRequest> for SubscribeRequest {
    fn from(val: LooperSubscribeRequest) -> Self {
        val.inner
    }
}
