use anyhow::Result;
use coinbase_mesh::models::NetworkIdentifier;

use crate::{ArchiveProvenance, CacheKey::NetworkId, MinaMesh, MinaMeshError};

impl MinaMesh {
  // Validate that the network identifier matches the network id of the GraphQL
  // server
  pub async fn validate_network(&self, network_identifier: &NetworkIdentifier) -> Result<(), MinaMeshError> {
    // Trustless mode (indexer history): validate against the configured network id — no daemon.
    if self.archive.provenance() == ArchiveProvenance::Verified {
      return self.compare_network_ids(&self.network_id, network_identifier);
    }

    // Check the cache
    if let Some(cached_network_id) = self.get_from_cache(NetworkId) {
      return self.compare_network_ids(&cached_network_id, network_identifier);
    }

    // Fetch from the node (daemon) if cache is empty or expired.
    let network_id = self.node.network_id().await?;
    self.insert_into_cache(NetworkId, network_id.clone());
    self.compare_network_ids(&network_id, network_identifier)
  }

  fn compare_network_ids(
    &self,
    fetched_network_id: &str,
    network_identifier: &NetworkIdentifier,
  ) -> Result<(), MinaMeshError> {
    let expected_network_id = format!("{}:{}", network_identifier.blockchain, network_identifier.network);

    if fetched_network_id != expected_network_id {
      Err(MinaMeshError::NetworkDne(expected_network_id, fetched_network_id.to_string()))
    } else {
      Ok(())
    }
  }
}
