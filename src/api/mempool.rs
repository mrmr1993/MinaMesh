// TODO: why does OCaml implementation query for the `daemon_status` and
// `initial_peers`?
#![allow(clippy::just_underscores_and_digits)]

use anyhow::Result;
use coinbase_mesh::models::{MempoolResponse, NetworkRequest, TransactionIdentifier};
use cynic::QueryBuilder;

use crate::{graphql::QueryMempool, MinaMesh};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/mempool.ml#L56
impl MinaMesh {
  pub async fn mempool(&self, req: NetworkRequest) -> Result<MempoolResponse> {
    self.validate_network(&req.network_identifier).await?;
    // Trustless backend: serve the mempool from the light node's gossip tap (still
    // best-effort — pending txs aren't proven — but no trusted daemon gatekeeper).
    if let Some(light_node) = &self.light_node {
      let view = light_node.mempool().await?;
      let hashes = view.transaction_ids.into_iter().map(TransactionIdentifier::new).collect();
      return Ok(MempoolResponse::new(hashes));
    }
    let QueryMempool { daemon_status: _0, initial_peers: _1, pooled_user_commands } =
      self.graphql_client.send(QueryMempool::build(())).await?;
    let hashes = pooled_user_commands
      .into_iter()
      .map(|command| TransactionIdentifier::new(command.hash.0))
      .collect::<Vec<TransactionIdentifier>>();
    Ok(MempoolResponse::new(hashes))
  }
}
