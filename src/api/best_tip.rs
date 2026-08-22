use cynic::QueryBuilder;

use crate::{graphql::QueryBestTip, MinaMesh};

impl MinaMesh {
  /// The state hash of the block the daemon considers the best tip, if it will
  /// tell us.
  ///
  /// Queries that have to choose between competing branches at the tip need to
  /// agree with the network about which branch wins. That decision is Mina's
  /// consensus rule -- for same-length chains, the greater blake2 digest of the
  /// last VRF output, then the greater state hash, and for long-range forks a
  /// comparison of virtual minimum window densities. None of that is expressible
  /// in Postgres, which has no blake2, so rather than approximate it we ask the
  /// node that already implements it.
  ///
  /// Returns `None` when the daemon cannot be reached or reports no chain, in
  /// which case callers fall back to their previous heuristic. A wrong-but-
  /// available answer is preferable to a failed balance lookup here, because the
  /// fallback is what the code did unconditionally until now.
  pub async fn best_tip_state_hash(&self) -> Option<String> {
    match self.graphql_client.send(QueryBestTip::build(())).await {
      Ok(response) => response.best_chain?.first().map(|block| block.state_hash.0.clone()),
      Err(err) => {
        tracing::warn!("Could not read the best tip from the daemon, falling back to the local heuristic: {}", err);
        None
      }
    }
  }
}
