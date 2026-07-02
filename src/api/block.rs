use coinbase_mesh::models::{BlockRequest, BlockResponse};

use crate::{MinaMesh, MinaMeshError};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/block.ml#L7
impl MinaMesh {
  pub async fn block(&self, request: BlockRequest) -> Result<BlockResponse, MinaMeshError> {
    self.validate_network(&request.network_identifier).await?;
    // The block + its commands come from the history axis (trustless indexer or Postgres
    // archive), assembled behind the `MinaArchive` trait.
    self.archive.block(&request.block_identifier).await
  }
}
