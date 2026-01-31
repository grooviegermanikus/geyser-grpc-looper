use std::collections::HashMap;
use solana_commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::SubscribeRequestFilterSlots;
use yellowstone_grpc_proto::prelude::SubscribeRequest;
use crate::yellow_util::map_commitment_level;

pub mod geyser_looper;
mod yellow_util;

pub struct LooperSubscribeRequest {
    inner: SubscribeRequest,
}

impl TryFrom<SubscribeRequest> for LooperSubscribeRequest {
    type Error = ();

    fn try_from(subscription: SubscribeRequest) -> Result<Self, Self::Error> {

        assert!(subscription.slots.is_empty(), "user must not request slots; but we will implicitly send confirmed slots");
        // force callers to set processed to avoid confusion
        assert_eq!(subscription.commitment, Some(map_commitment_level(CommitmentConfig::processed()) as i32));

        let magic_slots_subscription = SubscribeRequestFilterSlots { filter_by_commitment: None, interslot_updates: None };

        let subscribe_request = SubscribeRequest {
            slots: HashMap::from([
                ("_magic_confirmed_slots".to_string(), magic_slots_subscription),
            ]),
            commitment: Some(map_commitment_level(CommitmentConfig::processed()) as i32),
            ..subscription
        };

        Ok(Self { inner: subscribe_request})

    }
}

impl Into<SubscribeRequest> for LooperSubscribeRequest {
    fn into(self) -> SubscribeRequest {
        self.inner
    }
}
