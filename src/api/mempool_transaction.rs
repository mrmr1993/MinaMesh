use coinbase_mesh::models::{
  MempoolTransactionRequest, MempoolTransactionResponse, Transaction, TransactionIdentifier,
};

use crate::{generate_operations_user_command, MinaMesh, MinaMeshError};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/mempool.ml#L137
impl MinaMesh {
  pub async fn mempool_transaction(
    &self,
    request: MempoolTransactionRequest,
  ) -> Result<MempoolTransactionResponse, MinaMeshError> {
    self.validate_network(&request.network_identifier).await?;
    let hash = request.transaction_identifier.hash;
    // Route through the SAME live-node backend as `/mempool` (fixes the old 404: the light
    // path listed via the light node but get still hit the daemon — silently mainnet in
    // trustless mode). The light-node backend returns an honest `TransactionNotFound` until
    // its `/mempool/tx` endpoint lands; the daemon backend returns the real command.
    let Some(command) = self.node.mempool_transaction(&hash).await? else {
      return Err(MinaMeshError::TransactionNotFound(hash));
    };

    let operations = generate_operations_user_command(&command);

    Ok(MempoolTransactionResponse {
      metadata: None,
      transaction: Box::new(Transaction {
        operations,
        related_transactions: Some(vec![]),
        transaction_identifier: Box::new(TransactionIdentifier::new(hash)),
        metadata: None,
      }),
    })
  }
}
