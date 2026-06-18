use crate::yellow_util::map_commitment_level;
use crate::LooperError::{SlotsSubscriptionError, OnlyCommittedProcessedAllowed};
use solana_commitment_config::CommitmentConfig;
use std::collections::HashMap;
use yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots;
use yellowstone_grpc_proto::prelude::SubscribeRequest;

pub mod geyser_looper;
mod yellow_util;

#[derive(Clone, Debug, PartialEq)]
pub struct LooperSubscribeRequest {
    inner: SubscribeRequest,
}

#[derive(Debug, thiserror::Error)]
pub enum LooperError {
    #[error("LooperSubscribeRequest contains incompatible slot subscription")]
    SlotsSubscriptionError,

    #[error("LooperSubscribeRequest only supports CommitmentConfig::processed()")]
    OnlyCommittedProcessedAllowed,
}

impl TryFrom<SubscribeRequest> for LooperSubscribeRequest {
    type Error = LooperError;

    fn try_from(subscription: SubscribeRequest) -> Result<Self, Self::Error> {
        // force callers to set processed to avoid confusion
        if subscription.commitment
            != Some(map_commitment_level(CommitmentConfig::processed()) as i32)
        {
            return Err(OnlyCommittedProcessedAllowed);
        }

        let mut slots = subscription.slots.clone();
        if slots.contains_key("__magic_confirmed_slots") {
            return Err(SlotsSubscriptionError);
        }

        let magic_slots_subscription = SubscribeRequestFilterSlots {
            filter_by_commitment: None,
            interslot_updates: None,
        };
        slots.insert(
            "__magic_confirmed_slots".to_string(),
            magic_slots_subscription,
        );

        let subscribe_request = SubscribeRequest {
            slots,
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mangle_slot_subscritpions() {
        let subscription = SubscribeRequest {
            slots: HashMap::from([(
                "some_user_subscription".to_string(),
                SubscribeRequestFilterSlots {
                    filter_by_commitment: None,
                    interslot_updates: Some(true),
                },
            )]),
            commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
            ..SubscribeRequest::default()
        };

        let looper = LooperSubscribeRequest::try_from(subscription).unwrap();

        assert_eq!(looper.inner.slots.len(), 2);
    }

}