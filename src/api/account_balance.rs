use coinbase_mesh::models::{
  AccountBalanceRequest, AccountBalanceResponse, AccountIdentifier, Amount, BlockIdentifier,
};

use crate::{create_currency, MinaMesh, MinaMeshError, NodeProvenance};

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/account.ml#L11
impl MinaMesh {
  pub async fn account_balance(&self, req: AccountBalanceRequest) -> Result<AccountBalanceResponse, MinaMeshError> {
    self.validate_network(&req.network_identifier).await?;
    let AccountIdentifier { address, metadata, .. } = *req.account_identifier;
    match req.block_identifier {
      // Historical balance is a history-axis read (indexer staged ledger, or the Postgres
      // archive with vesting `timing_info`), assembled behind the `MinaArchive` trait.
      Some(block_identifier) => self.archive.historical_balance(&address, metadata, &block_identifier).await,
      None => self.frontier_balance(address).await,
    }
  }

  async fn frontier_balance(&self, public_key: String) -> Result<AccountBalanceResponse, MinaMeshError> {
    // The live account state comes from the node behind the trait — light node (Merkle-proved
    // epoch-ledger balance anchored to the verified tip; whole balance reported liquid, no
    // vesting split) or daemon (staged-tip balance with the liquid/locked split). The
    // provenance decides only the trustless metadata markers, so each path stays byte-identical.
    let acct = self.node.account(&public_key, None).await?.ok_or(MinaMeshError::AccountNotFound(public_key))?;
    let verified = self.node.provenance() == NodeProvenance::Verified;
    let metadata = if verified {
      serde_json::json!({
        "created_via_historical_lookup": false,
        "nonce": acct.nonce.to_string(),
        "trustless": true,
        // The light node proves balances against the finalized epoch ledger peers serve.
        "ledger": acct.ledger.clone().unwrap_or_default()
      })
    } else {
      serde_json::json!({
        "created_via_historical_lookup": false,
        "nonce": format!("{}", acct.nonce)
      })
    };
    // Trusted daemon reports the token id; the light node is MINA-only (default token).
    let currency = if verified { create_currency(None) } else { create_currency(Some(&acct.token_id)) };
    Ok(AccountBalanceResponse {
      block_identifier: Box::new(BlockIdentifier { hash: acct.block_state_hash, index: acct.block_height }),
      balances: vec![Amount {
        currency: Box::new(currency),
        value: acct.total_balance.to_string(),
        metadata: Some(serde_json::json!({
          "locked_balance": acct.locked_balance,
          "liquid_balance": acct.liquid_balance,
          "total_balance": acct.total_balance
        })),
      }],
      metadata: Some(metadata),
    })
  }
}
