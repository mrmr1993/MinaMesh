// TODO: get genesis block identifier from env

use coinbase_mesh::models::{BlockIdentifier, NetworkRequest, NetworkStatusResponse, Peer, SyncStatus};

use crate::{ArchiveProvenance, MinaMesh, MinaMeshError};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/network.ml#L201
impl MinaMesh {
  pub async fn network_status(&self, req: NetworkRequest) -> Result<NetworkStatusResponse, MinaMeshError> {
    self.validate_network(&req.network_identifier).await?;

    // The oldest block (archive availability floor) is a history-axis read in every mode.
    let oldest_block_identifier = self.archive.oldest_block_identifier().await?;

    // Trustless mode: current tip comes from the (indexer) history axis; the sync target is
    // the node's verified network tip (the light-node backend). No Mina daemon GraphQL.
    if self.archive.provenance() == ArchiveProvenance::Verified {
      let tip = self.archive.tip().await?;
      let current_index = tip.block_identifier.index;
      // Sync target = the node's proof-verified network tip. While the indexer backfills,
      // current < target ⇒ not yet synced.
      let target_index = self.node.status().await.map(|s| s.block_height).unwrap_or(current_index);
      let synced = current_index >= target_index - 2;
      return Ok(NetworkStatusResponse {
        // The light node holds a peer mesh but exposes only a count, not Rosetta Peer ids.
        peers: Some(vec![]),
        current_block_identifier: Box::new(tip.block_identifier),
        current_block_timestamp: tip.timestamp,
        genesis_block_identifier: Box::new(self.genesis_block_identifier.clone()),
        oldest_block_identifier: Some(Box::new(oldest_block_identifier)),
        sync_status: Some(Box::new(SyncStatus {
          current_index: Some(current_index),
          target_index: Some(target_index),
          stage: Some(if synced { "Synced".to_string() } else { "Catchup".to_string() }),
          synced: Some(synced),
        })),
      });
    }

    // Full mode: the live tip + peers + sync come from the node (daemon) via the trait.
    let status = self.node.status().await?;
    let sync =
      status.sync.map(|s| SyncStatus { stage: s.stage, synced: s.synced, current_index: None, target_index: None });
    Ok(NetworkStatusResponse {
      peers: Some(status.peers.into_iter().map(Peer::new).collect()),
      current_block_identifier: Box::new(BlockIdentifier::new(status.block_height, status.state_hash)),
      current_block_timestamp: status.utc_date_ms,
      genesis_block_identifier: Box::new(self.genesis_block_identifier.clone()),
      oldest_block_identifier: Some(Box::new(oldest_block_identifier)),
      sync_status: Some(Box::new(sync.unwrap_or(SyncStatus {
        stage: None,
        synced: None,
        current_index: None,
        target_index: None,
      }))),
    })
  }
}
