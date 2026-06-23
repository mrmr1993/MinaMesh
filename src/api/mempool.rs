use anyhow::Result;
use coinbase_mesh::models::{MempoolResponse, NetworkRequest, TransactionIdentifier};

use crate::MinaMesh;

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/mempool.ml#L56
impl MinaMesh {
  pub async fn mempool(&self, req: NetworkRequest) -> Result<MempoolResponse> {
    self.validate_network(&req.network_identifier).await?;
    // The live node — light node (gossip tap, verified) or daemon — behind one trait.
    let hashes = self.node.mempool().await?.into_iter().map(TransactionIdentifier::new).collect();
    Ok(MempoolResponse::new(hashes))
  }
}
