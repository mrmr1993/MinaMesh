//! One trait for "the node" (live surface), two interchangeable adapters.
//!
//! MinaMesh talks to the live chain — best/verified tip, mempool, live account state,
//! transaction submit — through the [`MinaNode`] trait. The trustless **light node** and
//! the trusted **full daemon** are interchangeable adapters behind it. This kills the old
//! `self.graphql_client.send(...)` (daemon) vs `if let Some(light_node)` (light) split that
//! drifted out of sync (the `/mempool/transaction` 404 bug) and silently fell back to public
//! mainnet in trustless mode.
//!
//! The **indexer** (historical blocks / accounts / search) is a *separate* axis and is not
//! part of this trait — history handlers keep calling the indexer (or Postgres) directly.

use async_trait::async_trait;
use cynic::{MutationBuilder, QueryBuilder};

use crate::{
  graphql::{
    self, Account3, AnnotatedBalance, Balance, GraphQLClient, Length, QueryBalance, QueryBalanceVariables,
    QueryMempool, QueryMempoolTransactions, QueryMempoolTransactionsVariables, QueryNetworkId, QueryNetworkStatus,
    SendDelegation, SendDelegationVariables, SendPayment, SendPaymentVariables, StateHash,
  },
  LightNodeClient, MinaMeshError, Payment, StakeDelegation, TransactionStatus, UserCommandOperationsData,
  UserCommandType,
};

/// How the caller knows a *node* response is true. This **must not** be flattened away —
/// it is the contract a verifiable-indexer proof envelope later rides on.
///
/// History has its own axis, [`crate::ArchiveProvenance`]. They are deliberately separate
/// types: a node cannot be backed by an archive and an archive cannot be backed by a daemon,
/// so one enum spanning both would let either report a provenance it cannot have.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeProvenance {
  /// Light node: SNARK-verified blocks, Merkle-proved balances, signature-checked mempool.
  Verified,
  /// Full daemon: you operate the node and trust it.
  Trusted,
}

/// The live best/verified tip plus sync progress. Genesis and the *oldest* block are
/// deliberately **not** here — genesis is resolved once at startup and oldest comes from
/// the indexer (history); those compose node-tip + indexer in the handler.
#[derive(Debug, Clone)]
pub struct NodeStatus {
  pub block_height: i64,
  pub state_hash: String,
  /// Block timestamp (utc_date), unix millis as parsed by the handler.
  pub utc_date_ms: i64,
  /// Rosetta peer ids, when the backend exposes them (the daemon does; the light node
  /// only exposes a count, so it returns an empty list).
  pub peers: Vec<String>,
  /// Sync stage / synced flag, when the backend reports it (daemon). The light node's
  /// tip is by construction the verified network tip, so it leaves this `None` and the
  /// handler composes sync state against the indexer.
  pub sync: Option<NodeSyncStatus>,
}

#[derive(Debug, Clone)]
pub struct NodeSyncStatus {
  pub stage: Option<String>,
  pub synced: Option<bool>,
}

/// Live balance + nonce for an account, with the block it is anchored to. Carries the
/// liquid/locked split when the backend provides it (daemon); the light node reports the
/// whole proof-anchored balance as liquid.
#[derive(Debug, Clone)]
pub struct NodeAccount {
  pub block_height: i64,
  pub block_state_hash: String,
  pub nonce: u64,
  pub token_id: String,
  pub total_balance: u64,
  pub liquid_balance: u64,
  pub locked_balance: u64,
  /// Which ledger a *verified* balance is proved against (light node, e.g. `staking_epoch`).
  /// `None` for the trusted daemon, which reports the staged-tip balance.
  pub ledger: Option<String>,
}

/// A single pending user command, in MinaMesh-neutral terms. Mirrors the indexer's
/// `IxUserCommand` shape so [`crate::generate_operations_user_command`] is reused verbatim:
/// `NodeUserCommand` implements [`UserCommandOperationsData`] (see below).
#[derive(Debug, Clone)]
pub struct NodeUserCommand {
  pub command_type: UserCommandType,
  pub fee_payer: String,
  pub source: String,
  pub receiver: String,
  pub nonce: i64,
  pub fee: u64,
  pub amount: Option<u64>,
  pub memo: Option<String>,
  pub hash: String,
  pub token: Option<String>,
}

/// Reuse `generate_operations_user_command` verbatim for pending txs by satisfying the
/// same trait the indexer/archive command types do.
impl UserCommandOperationsData for NodeUserCommand {
  fn command_type(&self) -> &UserCommandType {
    &self.command_type
  }

  fn fee_payer(&self) -> &str {
    &self.fee_payer
  }

  fn source(&self) -> &str {
    &self.source
  }

  fn receiver(&self) -> &str {
    &self.receiver
  }

  fn nonce(&self) -> i64 {
    self.nonce
  }

  fn memo(&self) -> Option<String> {
    self.memo.clone()
  }

  fn amount(&self) -> Option<String> {
    self.amount.map(|a| a.to_string())
  }

  fn fee(&self) -> String {
    self.fee.to_string()
  }

  // Pending mempool commands are not yet applied or failed; treat as Applied (the daemon's
  // mempool view reports no failure reason for pending txs).
  fn status(&self) -> Option<&TransactionStatus> {
    Some(&TransactionStatus::Applied)
  }

  fn failure_reason(&self) -> Option<&str> {
    None
  }

  fn creation_fee(&self) -> Option<&str> {
    None
  }

  fn token(&self) -> Option<&str> {
    self.token.as_deref()
  }
}

/// A signed payment ready to broadcast: the parsed [`Payment`] plus its raw signature.
pub struct SignedPayment<'a> {
  pub payment: &'a Payment,
  pub signature: &'a str,
  /// Pre-encoded binprot hex of the on-wire `MinaBaseUserCommandStableV2` for the gossip
  /// path (light node). The daemon path ignores this and re-sends the structured payment.
  pub tx_hex: &'a str,
}

/// A signed stake delegation ready to broadcast.
pub struct SignedDelegation<'a> {
  pub delegation: &'a StakeDelegation,
  pub signature: &'a str,
  pub tx_hex: &'a str,
}

/// The single live-node interface. The light node and the full daemon are adapters behind
/// it; in trustless mode there is **no** ambient daemon client to fall through to.
#[async_trait]
pub trait MinaNode: Send + Sync {
  /// `mina:<network>` — the Rosetta network id this node serves.
  async fn network_id(&self) -> Result<String, MinaMeshError>;
  /// Best/verified tip + sync state.
  async fn status(&self) -> Result<NodeStatus, MinaMeshError>;
  /// Live balance + nonce for `pubkey` (default token unless `token_id` is given).
  async fn account(&self, pubkey: &str, token_id: Option<&str>) -> Result<Option<NodeAccount>, MinaMeshError>;
  /// Pending transaction hashes.
  async fn mempool(&self) -> Result<Vec<String>, MinaMeshError>;
  /// A single pending transaction by hash, or `None` when not pending.
  async fn mempool_transaction(&self, hash: &str) -> Result<Option<NodeUserCommand>, MinaMeshError>;
  /// Broadcast a signed payment; returns the canonical tx hash.
  async fn submit_payment(&self, p: &SignedPayment<'_>) -> Result<String, MinaMeshError>;
  /// Broadcast a signed delegation; returns the canonical tx hash.
  async fn submit_delegation(&self, d: &SignedDelegation<'_>) -> Result<String, MinaMeshError>;

  /// How the caller knows these responses are true — must NOT be flattened away.
  fn provenance(&self) -> NodeProvenance;

  /// Escape hatch for the few daemon-only queries that are **not** part of the unified live
  /// surface (today: `construction/metadata`'s best-chain suggested-fee + genesis
  /// account-creation-fee, which the trustless path sources from the indexer/constants
  /// instead). Reachable only by downcasting to [`DaemonBackend`], so it cannot exist in
  /// trustless mode — no ambient daemon client leaks out.
  fn as_any(&self) -> &dyn std::any::Any;
}

// ---------------------------------------------------------------------------------------
// LightNodeBackend — wraps the existing trustless light-node HTTP client.
// ---------------------------------------------------------------------------------------

/// Trustless adapter: proof-anchored reads, peer-to-peer submit. `provenance() == Verified`.
#[derive(Debug)]
pub struct LightNodeBackend {
  client: LightNodeClient,
  network_id: String,
}

impl LightNodeBackend {
  pub fn new(client: LightNodeClient, network_id: String) -> Self {
    Self { client, network_id }
  }
}

#[async_trait]
impl MinaNode for LightNodeBackend {
  async fn network_id(&self) -> Result<String, MinaMeshError> {
    Ok(self.network_id.clone())
  }

  async fn status(&self) -> Result<NodeStatus, MinaMeshError> {
    let tip = self.client.tip().await?;
    Ok(NodeStatus {
      block_height: tip.height as i64,
      state_hash: tip.state_hash,
      // The light node's `/tip` doesn't carry a block timestamp; the live `/network/status`
      // path that uses the light node composes timestamp from the indexer tip, not here.
      utc_date_ms: 0,
      peers: vec![],
      sync: None,
    })
  }

  async fn account(&self, pubkey: &str, _token_id: Option<&str>) -> Result<Option<NodeAccount>, MinaMeshError> {
    let acct = self.client.account(pubkey).await?;
    // Merkle-proved finalized epoch-ledger balance, anchored to the verified tip. No vesting
    // split is available, so the whole balance is reported liquid.
    Ok(Some(NodeAccount {
      block_height: acct.anchored_height as i64,
      block_state_hash: acct.anchored_state_hash,
      nonce: acct.nonce as u64,
      token_id: crate::util::DEFAULT_TOKEN_ID.to_string(),
      total_balance: acct.balance,
      liquid_balance: acct.balance,
      locked_balance: 0,
      ledger: Some(acct.ledger),
    }))
  }

  async fn mempool(&self) -> Result<Vec<String>, MinaMeshError> {
    Ok(self.client.mempool().await?.transaction_ids)
  }

  async fn mempool_transaction(&self, hash: &str) -> Result<Option<NodeUserCommand>, MinaMeshError> {
    // The light node holds the full pending command in its `MempoolView` and decodes it for
    // us via `/mempool/tx?hash=` (B62 pks, nanomina amounts). `None` = not pending (404).
    let Some(tx) = self.client.mempool_transaction(hash).await? else {
      return Ok(None);
    };
    let command_type = match tx.kind.as_str() {
      "delegation" => UserCommandType::Delegation,
      _ => UserCommandType::Payment,
    };
    Ok(Some(NodeUserCommand {
      command_type,
      fee_payer: tx.fee_payer,
      source: tx.source,
      receiver: tx.receiver,
      nonce: tx.nonce as i64,
      fee: tx.fee,
      // A delegation has no transfer amount; report `None` rather than 0 to match the
      // indexer/daemon shape (`generate_operations_user_command` reads it as optional).
      amount: (command_type == UserCommandType::Payment).then_some(tx.amount),
      memo: Some(tx.memo),
      hash: tx.hash,
      token: None,
    }))
  }

  async fn submit_payment(&self, p: &SignedPayment<'_>) -> Result<String, MinaMeshError> {
    // Broadcast the signed command peer-to-peer. The canonical tx hash excludes the
    // signature, so it matches the daemon path's hash.
    Ok(self.client.submit(p.tx_hex).await?.tx_id)
  }

  async fn submit_delegation(&self, d: &SignedDelegation<'_>) -> Result<String, MinaMeshError> {
    Ok(self.client.submit(d.tx_hex).await?.tx_id)
  }

  fn provenance(&self) -> NodeProvenance {
    NodeProvenance::Verified
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}

// ---------------------------------------------------------------------------------------
// DaemonBackend — wraps the existing cynic GraphQL client + current queries.
// ---------------------------------------------------------------------------------------

/// Trusted adapter: the current hand-rolled `cynic` GraphQL queries against a Mina daemon.
/// `provenance() == TrustedDaemon`. This is the **only** holder of a `GraphQLClient` — there
/// is no ambient daemon client on `MinaMesh`, so it cannot exist in trustless mode.
#[derive(Debug)]
pub struct DaemonBackend {
  client: GraphQLClient,
}

impl DaemonBackend {
  pub fn new(client: GraphQLClient) -> Self {
    Self { client }
  }

  /// Map a raw submit error onto the structured `TransactionSubmit*` variants. Lifted from
  /// the old `construction_submit::map_error` (the daemon-specific GraphQL error strings).
  pub fn map_submit_error(err: MinaMeshError) -> MinaMeshError {
    match err {
      MinaMeshError::GraphqlMinaQuery(err) => {
        if err.contains("Couldn't infer nonce") {
          MinaMeshError::TransactionSubmitNoSender(err)
        } else if err.contains("less than the minimum fee") {
          MinaMeshError::TransactionSubmitFeeSmall(err)
        } else if err.contains("Invalid_signature") {
          MinaMeshError::TransactionSubmitInvalidSignature(err)
        } else if err.contains("below minimum_nonce") {
          MinaMeshError::TransactionSubmitBadNonce(err)
        } else if err.contains("Insufficient_funds") {
          MinaMeshError::TransactionSubmitInsufficientBalance(err)
        } else if err.contains("Expired") {
          MinaMeshError::TransactionSubmitExpired(err)
        } else {
          MinaMeshError::GraphqlMinaQuery(err)
        }
      }
      _ => err,
    }
  }
}

#[async_trait]
impl MinaNode for DaemonBackend {
  async fn network_id(&self) -> Result<String, MinaMeshError> {
    Ok(self.client.send(QueryNetworkId::build(())).await?.network_id)
  }

  async fn status(&self) -> Result<NodeStatus, MinaMeshError> {
    let QueryNetworkStatus { best_chain, daemon_status, sync_status } =
      self.client.send(QueryNetworkStatus::build(())).await?;
    let blocks = best_chain.ok_or(MinaMeshError::ChainInfoMissing)?;
    let first = blocks.into_iter().next().ok_or(MinaMeshError::ChainInfoMissing)?;
    let mesh_sync: coinbase_mesh::models::SyncStatus = sync_status.into();
    Ok(NodeStatus {
      block_height: first.protocol_state.consensus_state.block_height.0.parse::<i64>()?,
      state_hash: first.state_hash.0,
      utc_date_ms: first.protocol_state.blockchain_state.utc_date.0.parse::<i64>()?,
      peers: daemon_status.peers.into_iter().map(|p| p.peer_id).collect(),
      sync: Some(NodeSyncStatus { stage: mesh_sync.stage, synced: mesh_sync.synced }),
    })
  }

  async fn account(&self, pubkey: &str, _token_id: Option<&str>) -> Result<Option<NodeAccount>, MinaMeshError> {
    let result =
      self.client.send(QueryBalance::build(QueryBalanceVariables { public_key: pubkey.to_string().into() })).await?;
    if let QueryBalance {
      account:
        Some(Account3 {
          balance:
            AnnotatedBalance {
              block_height: Length(index_raw),
              state_hash: Some(StateHash(hash)),
              liquid: Some(Balance(liquid_raw)),
              total: Balance(total_raw),
            },
          nonce: Some(graphql::AccountNonce(nonce)),
          token_id: graphql::TokenId(token_id),
        }),
    } = result
    {
      let total = total_raw.parse::<u64>()?;
      let liquid = liquid_raw.parse::<u64>()?;
      Ok(Some(NodeAccount {
        block_height: index_raw.parse::<i64>()?,
        block_state_hash: hash,
        nonce: nonce.parse::<u64>().unwrap_or(0),
        token_id,
        total_balance: total,
        liquid_balance: liquid,
        locked_balance: total.saturating_sub(liquid),
        ledger: None,
      }))
    } else {
      Ok(None)
    }
  }

  async fn mempool(&self) -> Result<Vec<String>, MinaMeshError> {
    let QueryMempool { pooled_user_commands, .. } = self.client.send(QueryMempool::build(())).await?;
    Ok(pooled_user_commands.into_iter().map(|c| c.hash.0).collect())
  }

  async fn mempool_transaction(&self, hash: &str) -> Result<Option<NodeUserCommand>, MinaMeshError> {
    let QueryMempoolTransactions { pooled_user_commands, .. } = self
      .client
      .send(QueryMempoolTransactions::build(QueryMempoolTransactionsVariables { hashes: Some(vec![hash]) }))
      .await?;
    let Some(cmd) = pooled_user_commands.into_iter().next() else {
      return Ok(None);
    };
    // `kind` is "PAYMENT" | "STAKE_DELEGATION" (daemon casing).
    let command_type = match cmd.kind.0.to_ascii_uppercase().as_str() {
      "STAKE_DELEGATION" | "DELEGATION" => UserCommandType::Delegation,
      _ => UserCommandType::Payment,
    };
    Ok(Some(NodeUserCommand {
      command_type,
      fee_payer: cmd.source.public_key.0.clone(),
      source: cmd.source.public_key.0,
      receiver: cmd.receiver.public_key.0,
      nonce: cmd.nonce as i64,
      fee: cmd.fee.0.parse::<u64>().unwrap_or(0),
      amount: cmd.amount.0.parse::<u64>().ok(),
      memo: Some(cmd.memo),
      hash: cmd.hash.0,
      token: None,
    }))
  }

  async fn submit_payment(&self, p: &SignedPayment<'_>) -> Result<String, MinaMeshError> {
    let payment = p.payment;
    let variables = SendPaymentVariables {
      amount: payment.amount.into(),
      fee: payment.fee.into(),
      from: payment.from.clone().into(),
      to: payment.to.clone().into(),
      nonce: payment.nonce.into(),
      valid_until: payment.valid_until.map(|v| v.into()),
      memo: payment.memo.as_deref(),
      signature: p.signature,
    };
    self
      .client
      .send(SendPayment::build(variables))
      .await
      .map(|r| r.send_payment.payment.hash.0)
      .map_err(Self::map_submit_error)
  }

  async fn submit_delegation(&self, d: &SignedDelegation<'_>) -> Result<String, MinaMeshError> {
    let delegation = d.delegation;
    let variables = SendDelegationVariables {
      fee: delegation.fee.into(),
      from: delegation.delegator.clone().into(),
      to: delegation.new_delegate.clone().into(),
      nonce: delegation.nonce.into(),
      valid_until: delegation.valid_until.map(|v| v.into()),
      memo: delegation.memo.as_deref(),
      signature: d.signature,
    };
    self
      .client
      .send(SendDelegation::build(variables))
      .await
      .map(|r| r.send_delegation.delegation.hash.0)
      .map_err(Self::map_submit_error)
  }

  fn provenance(&self) -> NodeProvenance {
    NodeProvenance::Trusted
  }

  fn as_any(&self) -> &dyn std::any::Any {
    self
  }
}

impl DaemonBackend {
  /// The daemon-only `construction/metadata` query: suggested-fee inputs (best chain) +
  /// genesis account-creation-fee + sender nonce + receiver existence, in one round-trip.
  /// Not part of the unified live surface (the trustless path computes these from the
  /// indexer + protocol constants), so it lives here on the daemon adapter.
  pub async fn construction_metadata_query(
    &self,
    sender: &str,
    receiver: &str,
  ) -> Result<graphql::QueryConstructionMetadata, MinaMeshError> {
    use crate::util::DEFAULT_TOKEN_ID;
    let query = graphql::QueryConstructionMetadata::build(graphql::QueryConstructionMetadataVariables {
      sender: graphql::PublicKey(sender.to_string()),
      // nonce is based on the fee payer's account using the default token ID.
      token_id: Some(graphql::TokenId(DEFAULT_TOKEN_ID.to_string())),
      receiver_key: graphql::PublicKey(receiver.to_string()),
    });
    self.client.send(query).await
  }
}
