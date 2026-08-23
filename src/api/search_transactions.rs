use coinbase_mesh::models::{SearchTransactionsRequest, SearchTransactionsResponse};

use crate::{MinaMesh, MinaMeshError};

impl MinaMesh {
  pub async fn search_transactions(
    &self,
    req: SearchTransactionsRequest,
  ) -> Result<SearchTransactionsResponse, MinaMeshError> {
    self.validate_network(&req.network_identifier).await?;
    // Transaction search is a history-axis read. The Postgres archive pages over user +
    // internal + zkApp commands with real offsets/total_count; the trustless indexer emulates
    // search over user commands only (no offset pagination). Both live behind `MinaArchive`.
    let include_timestamp = req.include_timestamp.unwrap_or(false);
    Ok(self.archive.search_transactions(&req).await?.into_response(include_timestamp))
  }
}
