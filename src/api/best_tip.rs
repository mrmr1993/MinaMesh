use cynic::QueryBuilder;

use crate::{
  graphql::{QueryBestTip, QueryBestTipVariables},
  MinaMesh,
};

/// How much of the daemon's best chain to consider when looking for a tip the
/// archive also has. An archive trails the daemon it follows, usually by a block
/// or two, so taking only the very tip would fall back to the local heuristic
/// almost every time.
const BEST_CHAIN_CANDIDATES: i32 = 16;

impl MinaMesh {
  /// The deepest block the daemon considers part of its best chain that this
  /// archive also holds, if there is one.
  ///
  /// Queries that choose between competing branches at the tip need to agree
  /// with the network about which branch wins. That decision is Mina's consensus
  /// rule -- for same-length chains, the greater blake2 digest of the last VRF
  /// output, then the greater state hash, and for long-range forks a comparison
  /// of virtual minimum window densities. None of it is expressible in Postgres,
  /// which has no blake2, so rather than approximate it we ask the node that
  /// already implements it.
  ///
  /// The daemon's answer is only useful if the archive has the block, since it
  /// is used to seed a walk through the archive's own `parent_id` links. The
  /// archive is always at least slightly behind, so we ask for a run of recent
  /// blocks and take the first one present rather than the tip alone.
  ///
  /// Returns `None` when the daemon cannot be reached, reports no chain, or
  /// reports only blocks the archive has not caught up with. Callers then fall
  /// back to their previous heuristic: a wrong-but-available answer is
  /// preferable to a failed balance lookup, and the fallback is what the code
  /// did unconditionally until now.
  pub async fn best_tip_state_hash(&self) -> Option<String> {
    let candidates: Vec<String> = match self
      .graphql_client
      .send(QueryBestTip::build(QueryBestTipVariables { max_length: Some(BEST_CHAIN_CANDIDATES) }))
      .await
    {
      Ok(response) => response.best_chain?.into_iter().map(|block| block.state_hash.0).collect(),
      Err(err) => {
        tracing::warn!("Could not read the best chain from the daemon, falling back to the local heuristic: {}", err);
        return None;
      }
    };
    if candidates.is_empty() {
      return None;
    }
    // bestChain returns oldest first, so reverse to prefer the tip.
    let preference: Vec<String> = candidates.into_iter().rev().collect();
    match sqlx::query_file_scalar!("sql/queries/best_tip_in_archive.sql", &preference)
      .fetch_optional(&self.pg_pool)
      .await
    {
      Ok(found) => {
        if found.is_none() {
          tracing::warn!(
            "The archive holds none of the daemon's {} most recent blocks; falling back to the local heuristic",
            preference.len()
          );
        }
        found.flatten()
      }
      Err(err) => {
        tracing::warn!("Could not check the daemon's best chain against the archive: {}", err);
        None
      }
    }
  }
}
