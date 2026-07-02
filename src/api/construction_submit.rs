use anyhow::Result;
use coinbase_mesh::models::{ConstructionSubmitRequest, TransactionIdentifier, TransactionIdentifierResponse};
use mina_p2p_messages::binprot::BinProtWrite;

use crate::{MinaMesh, MinaMeshError, Payment, Provenance, SignedDelegation, SignedPayment, TransactionSigned};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/construction.ml#L849
impl MinaMesh {
  pub async fn construction_submit(
    &self,
    request: ConstructionSubmitRequest,
  ) -> Result<TransactionIdentifierResponse, MinaMeshError> {
    self.validate_network(&request.network_identifier).await?;

    let signed_transaction = TransactionSigned::from_json_string(&request.signed_transaction)?;
    if signed_transaction.payment.is_some() && signed_transaction.stake_delegation.is_some() {
      return Err(MinaMeshError::JsonParse(Some(
        "Signed transaction must have one of: payment, stake_delegation".to_string(),
      )));
    }

    // The on-wire binprot hex is the gossip-topic format (light node) and is ignored by the
    // daemon backend. The canonical tx hash excludes the signature, so both delivery paths
    // yield the same hash.
    let user_command = self.signed_user_command(&signed_transaction)?;
    let mut bytes = Vec::new();
    user_command
      .binprot_write(&mut bytes)
      .map_err(|e| MinaMeshError::Exception(format!("binprot encode user command: {e}")))?;
    let tx_hex = hex::encode(bytes);
    let signature = signed_transaction.signature.clone();

    if let Some(payment) = &signed_transaction.payment {
      tracing::info!("Payment transaction");
      let result = self.node.submit_payment(&SignedPayment { payment, signature: &signature, tx_hex: &tx_hex }).await;
      let hash = match result {
        Ok(hash) => hash,
        Err(e) => return Err(self.enrich_submit_error(e, &signature, Some(payment.clone())).await),
      };
      self.cache_transaction(&signature);
      tracing::info!("Success! Transaction hash: {}", hash);
      Ok(TransactionIdentifierResponse::new(TransactionIdentifier::new(hash)))
    } else if let Some(delegation) = &signed_transaction.stake_delegation {
      tracing::info!("Stake delegation transaction");
      let result =
        self.node.submit_delegation(&SignedDelegation { delegation, signature: &signature, tx_hex: &tx_hex }).await;
      let hash = match result {
        Ok(hash) => hash,
        Err(e) => return Err(self.enrich_submit_error(e, &signature, None).await),
      };
      self.cache_transaction(&signature);
      tracing::info!("Success! Transaction hash: {}", hash);
      Ok(TransactionIdentifierResponse::new(TransactionIdentifier::new(hash.to_string())))
    } else {
      tracing::debug!("Signed transaction missing payment or stake delegation");
      Err(MinaMeshError::JsonParse(Some("Signed transaction missing payment or stake delegation".to_string())))
    }
  }

  /// Refine a submit error with cache/DB duplicate detection. The `DaemonBackend` already
  /// maps the raw GraphQL error strings onto `TransactionSubmit*`; here we add the
  /// duplicate-vs-bad-nonce disambiguation that needs MinaMesh's cache and archive. Only the
  /// trusted daemon produces these structured submit errors — the light node's peer-to-peer
  /// submit doesn't, so this is a no-op for `Provenance::Verified`.
  async fn enrich_submit_error(
    &self,
    err: MinaMeshError,
    signed_tx_str: &str,
    payment: Option<Payment>,
  ) -> MinaMeshError {
    if self.node.provenance() != Provenance::TrustedDaemon {
      return err;
    }
    if let MinaMeshError::TransactionSubmitBadNonce(ref msg) = err {
      if self.is_transaction_cached(signed_tx_str) {
        return MinaMeshError::TransactionSubmitDuplicate(msg.clone());
      }
      if let Some(payment) = payment {
        if self.is_transaction_in_db(payment).await.unwrap_or(false) {
          return MinaMeshError::TransactionSubmitDuplicate("Transaction already in database".to_string());
        }
      }
    }
    err
  }

  async fn is_transaction_in_db(&self, payment: Payment) -> Result<bool, MinaMeshError> {
    // Duplicate detection is a history-axis read: the indexer scans the sender's recent
    // commands, the Postgres archive matches the exact payment row. Both live behind `MinaArchive`.
    self.archive.payment_in_history(&payment).await
  }
}
