//! Client for the trustless **mina-light-node** HTTP surface.
//!
//! When configured (`MINAMESH_LIGHT_NODE_URL`), MinaMesh can serve the live-state
//! endpoints — mempool, frontier balance, transaction submit — from the trustless
//! light node instead of a trusted GraphQL daemon. The light node Merkle-proves
//! balances against a recursively-verified ledger root and submits via peer-to-peer
//! gossip, so these reads/writes no longer trust a single gatekeeper. Historical
//! reads (`/block`, archived balances, search) stay on the archive database.

use serde::Deserialize;

use crate::MinaMeshError;

/// HTTP client for a running `mina-light-node-server`.
#[derive(Debug, Clone)]
pub struct LightNodeClient {
  base_url: String,
  http: reqwest::Client,
}

#[derive(Debug, Deserialize)]
pub struct LightTip {
  pub network: String,
  pub height: u32,
  pub state_hash: String,
  pub staking_epoch_ledger_hash: String,
}

#[derive(Debug, Deserialize)]
pub struct LightMempool {
  pub count: usize,
  pub transaction_ids: Vec<String>,
}

#[derive(Debug, Deserialize)]
pub struct LightAccount {
  pub public_key: String,
  pub balance: u64,
  pub nonce: u32,
  pub anchored_height: u32,
  pub anchored_state_hash: String,
  /// Which ledger the balance is proved against (e.g. `staking_epoch`).
  pub ledger: String,
}

#[derive(Debug, Deserialize)]
pub struct LightSubmit {
  pub tx_id: String,
  pub published: bool,
  pub echoes: usize,
}

impl LightNodeClient {
  pub fn new(base_url: String) -> Self {
    Self { base_url: base_url.trim_end_matches('/').to_string(), http: reqwest::Client::new() }
  }

  async fn get_json<T: for<'de> Deserialize<'de>>(&self, path: &str) -> Result<T, MinaMeshError> {
    let url = format!("{}{}", self.base_url, path);
    let resp = self
      .http
      .get(&url)
      .send()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("light-node GET {path}: {e}")))?;
    if !resp.status().is_success() {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      return Err(MinaMeshError::Exception(format!("light-node GET {path} -> {status}: {body}")));
    }
    resp
      .json::<T>()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("light-node GET {path} decode: {e}")))
  }

  /// The verified best tip (height + epoch-ledger root).
  pub async fn tip(&self) -> Result<LightTip, MinaMeshError> {
    self.get_json("/tip").await
  }

  /// Best-effort pending transaction hashes from the gossip tap.
  pub async fn mempool(&self) -> Result<LightMempool, MinaMeshError> {
    self.get_json("/mempool").await
  }

  /// Proof-anchored balance + nonce for `pubkey`. The light node resolves the leaf index
  /// from its own swept map and Merkle-proves the account against the verified epoch root.
  pub async fn account(&self, pubkey: &str) -> Result<LightAccount, MinaMeshError> {
    self.get_json(&format!("/account?pubkey={pubkey}")).await
  }

  /// Broadcast a signed `MinaBaseUserCommandStableV2` (hex binprot) to the tx-pool
  /// gossip topic.
  pub async fn submit(&self, tx_hex: &str) -> Result<LightSubmit, MinaMeshError> {
    let url = format!("{}/submit", self.base_url);
    let resp = self
      .http
      .post(&url)
      .json(&serde_json::json!({ "tx_hex": tx_hex }))
      .send()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("light-node POST /submit: {e}")))?;
    if !resp.status().is_success() {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      return Err(MinaMeshError::Exception(format!("light-node POST /submit -> {status}: {body}")));
    }
    resp
      .json::<LightSubmit>()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("light-node POST /submit decode: {e}")))
  }
}
