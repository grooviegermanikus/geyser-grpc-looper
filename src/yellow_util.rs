use solana_commitment_config::CommitmentConfig;
use yellowstone_grpc_proto::geyser::CommitmentLevel;

pub(crate) const fn map_commitment_level(commitment_config: CommitmentConfig) -> CommitmentLevel {
    // solana_sdk -> yellowstone
    match commitment_config.commitment {
        solana_commitment_config::CommitmentLevel::Processed => CommitmentLevel::Processed,
        solana_commitment_config::CommitmentLevel::Confirmed => CommitmentLevel::Confirmed,
        solana_commitment_config::CommitmentLevel::Finalized => CommitmentLevel::Finalized,
    }
}

