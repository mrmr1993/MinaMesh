//! One trait for "the archive" (historical surface), interchangeable adapters behind it.
//!
//! MinaMesh serves the HISTORICAL/archive reads — `/block`, historical `/account/balance`,
//! `/search/transactions`, the network oldest block, construction nonce/duplicate lookups —
//! through the [`MinaArchive`] trait. This is the **history** axis, orthogonal to the live
//! [`crate::MinaNode`] axis (tip / mempool / submit / frontier balance).
//!
//! Adapters:
//!   * [`IndexerArchive`]  — the trustless `mina-indexer` (SNARK-gated ingestion). `Verified`.
//!   * [`PostgresArchive`] — a raw Mina archive Postgres (the rosetta SQL). `TrustedArchive`.
//!
//! A future `archive-node-api` adapter is a third implementation of this same trait (it can
//! only serve part of the surface — see docs — so it would return `Unsupported` for the rest).
//!
//! This mirrors what [`crate::MinaNode`] did for the live axis: it collapses the per-handler
//! `if let Some(indexer) { … } else { …Postgres SQL… }` forks into one selected adapter, so
//! the two paths can no longer drift out of sync. The assembly for each backend lives here.

use async_trait::async_trait;
use coinbase_mesh::models::{
  AccountBalanceResponse, Amount, Block, BlockIdentifier, BlockResponse, BlockTransaction, PartialBlockIdentifier,
  SearchTransactionsRequest, SearchTransactionsResponse, Transaction, TransactionIdentifier,
};
use serde::Serialize;
use serde_json::{json, Value};
use sqlx::{FromRow, PgPool};

use crate::{
  create_currency, generate_internal_command_transaction_identifier, generate_operations_internal_command,
  generate_operations_user_command, generate_operations_zkapp_command, generate_transaction_metadata,
  util::{Wrapper, DEFAULT_TOKEN_ID},
  ChainStatus, IndexerClient, InternalCommand, InternalCommandMetadata, InternalCommandType, IxSearchTxn,
  MinaMeshError, Payment, Provenance, TransactionStatus, UserCommand, UserCommandMetadata, UserCommandType,
  ZkAppCommand,
};

/// The Mina account-creation fee (nanomina) — a protocol constant (1 MINA) on these networks.
const ACCOUNT_CREATION_FEE: u64 = 1_000_000_000;

/// A chain tip as the history axis sees it: block identifier + timestamp (unix millis).
#[derive(Debug, Clone)]
pub struct ArchiveTip {
  pub block_identifier: BlockIdentifier,
  pub timestamp: i64,
}

/// The single historical-read interface. The trustless indexer and a raw archive Postgres
/// are interchangeable adapters behind it. Handlers compose these history reads with the
/// live [`crate::MinaNode`] where an endpoint spans both axes (e.g. `/network/status`).
/// A block as the history axis knows it: its identity, and the commands it carries in the
/// backend-neutral shapes the operation generators already consume.
///
/// Adapters return this rather than a `BlockResponse` so that Rosetta assembly happens once,
/// above the trait. An adapter answers only "what does this backend hold?"; how that is
/// expressed as operations is not its business, and cannot drift between backends.
#[derive(Debug)]
pub struct ArchiveBlock {
  pub block_identifier: BlockIdentifier,
  pub parent_block_identifier: BlockIdentifier,
  /// Block timestamp, unix millis.
  pub timestamp: i64,
  pub creator: Option<String>,
  pub user_commands: Vec<UserCommandMetadata>,
  pub internal_commands: Vec<InternalCommandMetadata>,
  /// One entry per zkApp account update; see [`zkapp_commands_to_transactions`].
  pub zkapp_commands: Vec<ZkAppCommand>,
}

impl From<ArchiveBlock> for BlockResponse {
  fn from(block: ArchiveBlock) -> Self {
    // Internal commands first, then user, then zkApp -- the order the Postgres adapter has
    // always produced.
    let mut transactions: Vec<Transaction> = block.internal_commands.iter().map(internal_command_transaction).collect();
    transactions.extend(block.user_commands.iter().map(|meta| Transaction {
      transaction_identifier: Box::new(TransactionIdentifier::new(meta.hash.clone())),
      metadata: generate_transaction_metadata(meta),
      operations: generate_operations_user_command(meta),
      related_transactions: None,
    }));
    transactions.extend(zkapp_commands_to_transactions(block.zkapp_commands));

    BlockResponse {
      block: Some(Box::new(Block {
        block_identifier: Box::new(block.block_identifier),
        parent_block_identifier: Box::new(block.parent_block_identifier),
        timestamp: block.timestamp,
        transactions,
        metadata: block.creator.map(|creator| json!({ "creator": creator })),
      })),
      other_transactions: None,
    }
  }
}

/// An account's balance at a historical block, as the history axis knows it. Adapters report
/// the numbers; how they are expressed as a Rosetta `AccountBalanceResponse` is decided once,
/// above the trait.
///
/// Adapters return `Option<ArchiveAccountBalance>`: `None` says the account does not exist at
/// that block, which is a real answer a full-history archive can give. It is *not* the same as
/// being unable to see the account, which an archive holding a window of history can hit and
/// which is [`MinaMeshError::AccountNotVisible`]. Collapsing the two -- reporting a zero balance
/// because nothing was found -- tells a caller an account is empty when the truth may be that it
/// is merely older than the blocks held.
#[derive(Debug, Clone)]
pub enum ArchiveAccountBalance {
  Found {
    block_identifier: BlockIdentifier,
    /// The token the balance is denominated in. Always known for an account that exists; the
    /// case where it was not is now [`ArchiveAccountBalance::Absent`].
    token_id: String,
    total_balance: u64,
    liquid_balance: u64,
    locked_balance: u64,
    nonce: u64,
  },
  /// The account does not exist at that block. Rosetta expresses this as a zero balance, but
  /// only an adapter that can see the whole history may say it.
  Absent { block_identifier: BlockIdentifier },
}

impl From<ArchiveAccountBalance> for AccountBalanceResponse {
  fn from(balance: ArchiveAccountBalance) -> Self {
    match balance {
      ArchiveAccountBalance::Found {
        block_identifier,
        token_id,
        total_balance,
        liquid_balance,
        locked_balance,
        nonce,
      } => AccountBalanceResponse {
        block_identifier: Box::new(block_identifier),
        balances: vec![Amount {
          currency: Box::new(create_currency(Some(&token_id))),
          // Rosetta's `value` is the spendable balance; the locked/liquid/total split rides in
          // metadata, as it has since the OCaml implementation.
          value: liquid_balance.to_string(),
          metadata: Some(json!({
            "locked_balance": locked_balance,
            "liquid_balance": liquid_balance,
            "total_balance": total_balance
          })),
        }],
        metadata: Some(json!({
          "created_via_historical_lookup": true,
          "nonce": nonce.to_string()
        })),
      },
      // Byte-for-byte what the previous not-found path produced: an absent account is still
      // reported as a zero balance in the default token, with the same metadata shape. Only the
      // adapters' obligation changed, not the response.
      ArchiveAccountBalance::Absent { block_identifier } => AccountBalanceResponse {
        block_identifier: Box::new(block_identifier),
        balances: vec![Amount {
          currency: Box::new(create_currency(None)),
          value: "0".to_string(),
          metadata: Some(json!({ "locked_balance": 0, "liquid_balance": 0, "total_balance": 0 })),
        }],
        metadata: Some(json!({ "created_via_historical_lookup": true, "nonce": "0" })),
      },
    }
  }
}

/// One search hit: the command, and which block it was found in.
#[derive(Debug)]
pub struct ArchiveSearchCommand {
  pub block_identifier: BlockIdentifier,
  /// Block timestamp in unix millis, when the backend can supply one.
  pub timestamp: Option<i64>,
  pub command: ArchiveSearchCommandKind,
}

#[derive(Debug)]
pub enum ArchiveSearchCommandKind {
  User(UserCommandMetadata),
  Internal(InternalCommandMetadata),
}

/// A page of search results, as the history axis knows it. Adapters own searching, filtering
/// and pagination -- those are genuinely backend-specific -- but not how a hit is expressed as
/// a Rosetta `BlockTransaction`, which is decided once by [`ArchiveTransactionPage::into_response`].
#[derive(Debug)]
pub struct ArchiveTransactionPage {
  pub commands: Vec<ArchiveSearchCommand>,
  /// zkApp hits arrive as one row per account update and are grouped during assembly.
  pub zkapp_commands: Vec<ZkAppCommand>,
  pub total_count: i64,
  pub next_offset: Option<i64>,
}

impl From<ArchiveSearchCommand> for BlockTransaction {
  fn from(hit: ArchiveSearchCommand) -> Self {
    let transaction = match &hit.command {
      ArchiveSearchCommandKind::User(meta) => Transaction {
        transaction_identifier: Box::new(TransactionIdentifier::new(meta.hash.clone())),
        operations: generate_operations_user_command(meta),
        metadata: generate_transaction_metadata(meta),
        related_transactions: None,
      },
      ArchiveSearchCommandKind::Internal(meta) => internal_command_transaction(meta),
    };
    BlockTransaction::new(hit.block_identifier, transaction)
  }
}

impl ArchiveTransactionPage {
  /// Assemble the page. `include_timestamp` is a property of the request, not of the backend,
  /// so it is applied here rather than threaded into every adapter.
  pub fn into_response(self, include_timestamp: bool) -> SearchTransactionsResponse {
    let mut transactions: Vec<BlockTransaction> = self
      .commands
      .into_iter()
      .map(|hit| {
        let timestamp = hit.timestamp;
        let mut bt: BlockTransaction = hit.into();
        bt.timestamp = if include_timestamp { timestamp } else { None };
        bt
      })
      .collect();
    transactions.extend(zkapp_commands_to_block_transactions(self.zkapp_commands, include_timestamp));
    SearchTransactionsResponse { transactions, total_count: self.total_count, next_offset: self.next_offset }
  }
}

/// Whether a payment is already on chain.
///
/// Not a boolean, because the third answer is real and both collapses of it are harmful. Reading
/// "I cannot tell" as *applied* refuses a legitimate resubmit of a payment that was orphaned;
/// reading it as *absent* invites a double spend. A full-history archive never returns
/// [`PaymentHistory::Unknown`]; one holding a window does whenever the payment would predate the
/// blocks it holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PaymentHistory {
  /// Found on the chain that survived.
  Applied,
  /// Certainly not applied. A windowed archive may still say this when the sender's nonce has
  /// not reached the payment's, which rules it out without needing to see the blocks.
  Absent,
  /// Cannot be determined from what this archive holds.
  Unknown,
}

#[async_trait]
pub trait MinaArchive: Send + Sync {
  /// How the caller knows these history responses are true. `Verified` for the SNARK-gated
  /// indexer, `TrustedArchive` for a Postgres archive you operate.
  fn provenance(&self) -> Provenance;

  /// The current canonical tip (height + state hash + timestamp millis).
  async fn tip(&self) -> Result<ArchiveTip, MinaMeshError>;

  /// The earliest canonical block held — the Rosetta `oldest_block` (archive availability floor).
  async fn oldest_block_identifier(&self) -> Result<BlockIdentifier, MinaMeshError>;

  /// The block at the given height / state hash / best tip, with the commands it carries.
  /// Rosetta assembly is done once by `BlockResponse::from`, not by each adapter.
  async fn block(&self, partial: &PartialBlockIdentifier) -> Result<ArchiveBlock, MinaMeshError>;

  /// Historical balance + nonce for `public_key` at the block named by `partial`. Rosetta
  /// assembly is done once by `AccountBalanceResponse::from`, not by each adapter.
  async fn historical_balance(
    &self,
    public_key: &str,
    metadata: Option<Value>,
    partial: &PartialBlockIdentifier,
  ) -> Result<ArchiveAccountBalance, MinaMeshError>;

  /// Whether `/account/balance` can be answered for *any* account at a block in range, which is
  /// what `Allow.historical_balance_lookup` promises a client.
  ///
  /// A full-history archive can, so this defaults to true. An archive holding a window of blocks
  /// cannot without a ledger snapshot at its floor: a block records only the accounts it
  /// touched, so one that has not moved recently is invisible even though the block the client
  /// asked about is in range.
  fn historical_balance_lookup(&self) -> bool {
    true
  }

  /// Search historical transactions. Rosetta assembly, and the request's `include_timestamp`,
  /// are applied once by [`ArchiveTransactionPage::into_response`].
  async fn search_transactions(&self, req: &SearchTransactionsRequest)
    -> Result<ArchiveTransactionPage, MinaMeshError>;

  /// The best (latest) account nonce, or `None` if the account doesn't exist yet
  /// (`construction/metadata`: current nonce + receiver existence ⇒ creation fee).
  async fn account_nonce(&self, public_key: &str) -> Result<Option<u32>, MinaMeshError>;

  /// Whether an exact-match `payment` already exists in history (submit duplicate detection).
  async fn payment_in_history(&self, payment: &Payment) -> Result<PaymentHistory, MinaMeshError>;
}

// ===========================================================================================
// IndexerArchive — trustless mina-indexer adapter. provenance() == Verified.
// ===========================================================================================

/// Trustless history adapter over the `mina-indexer` GraphQL surface. The indexer ingests a
/// block only after its Pickles/kimchi SNARK proof verifies, so these reads trust math, not
/// whoever served the block.
#[derive(Debug)]
pub struct IndexerArchive {
  client: IndexerClient,
}

impl IndexerArchive {
  pub fn new(client: IndexerClient) -> Self {
    Self { client }
  }

  /// Resolve the indexer block named by a partial identifier (hash wins, else height, else tip).
  async fn resolve_block(&self, partial: &PartialBlockIdentifier) -> Result<crate::IxBlock, MinaMeshError> {
    match (&partial.hash, partial.index) {
      (Some(h), _) => self.client.block(None, Some(h)).await?,
      (None, Some(idx)) => self.client.block(Some(idx), None).await?,
      (None, None) => self.client.block(Some(self.client.tip().await?.block_height as i64), None).await?,
    }
    .ok_or_else(|| MinaMeshError::BlockMissing(partial.index, partial.hash.clone()))
  }
}

#[async_trait]
impl MinaArchive for IndexerArchive {
  fn provenance(&self) -> Provenance {
    Provenance::Verified
  }

  async fn tip(&self) -> Result<ArchiveTip, MinaMeshError> {
    let t = self.client.tip().await?;
    Ok(ArchiveTip {
      block_identifier: BlockIdentifier::new(t.block_height as i64, t.state_hash),
      timestamp: t.protocol_state.blockchain_state.utc_date.parse::<i64>()?,
    })
  }

  async fn oldest_block_identifier(&self) -> Result<BlockIdentifier, MinaMeshError> {
    let o = self.client.oldest().await?;
    Ok(BlockIdentifier::new(o.block_height as i64, o.state_hash))
  }

  /// Build a Rosetta block from the trustless indexer. Reuses the same operation generators
  /// as the Postgres path by mapping the indexer's block into the `UserCommandMetadata` /
  /// `InternalCommandMetadata` shapes. Degradations vs Postgres: internal-command transaction
  /// identifiers are synthesized from the block hash (the indexer doesn't expose internal-command
  /// hashes); zkApp commands are not itemized.
  async fn block(&self, partial: &PartialBlockIdentifier) -> Result<ArchiveBlock, MinaMeshError> {
    let ix = self.resolve_block(partial).await?;

    let block_identifier = BlockIdentifier::new(ix.block_height as i64, ix.state_hash.clone());
    // Parent links to the previous state hash at height-1; genesis links to itself.
    let parent_block_identifier = if ix.block_height <= 1 {
      block_identifier.clone()
    } else {
      BlockIdentifier::new(ix.block_height as i64 - 1, ix.protocol_state.previous_state_hash.clone())
    };
    let timestamp: i64 = ix.protocol_state.blockchain_state.utc_date.parse()?;

    let mut user_commands: Vec<UserCommandMetadata> = Vec::new();
    let mut internal_commands: Vec<InternalCommandMetadata> = Vec::new();

    // User commands (payments / delegations).
    for uc in &ix.transactions.user_commands {
      let command_type =
        if uc.kind.to_uppercase().contains("DELEG") { UserCommandType::Delegation } else { UserCommandType::Payment };
      let amount = match command_type {
        UserCommandType::Payment => Some(uc.amount.to_string()),
        UserCommandType::Delegation => None,
      };
      let meta = UserCommandMetadata {
        command_type,
        nonce: uc.nonce as i64,
        amount,
        fee: Some(uc.fee.to_string()),
        valid_until: None,
        memo: Some(uc.memo.clone()),
        hash: uc.hash.clone(),
        fee_payer: uc.from.clone(),
        source: uc.from.clone(),
        receiver: uc.to.clone().unwrap_or_default(),
        status: if uc.is_applied { TransactionStatus::Applied } else { TransactionStatus::Failed },
        failure_reason: uc.failure_reason.clone(),
        // 1 MINA account-creation fee when this payment created the receiver (matches the
        // Postgres `accounts_created` attribution; the generator negates it on the receiver).
        creation_fee: uc.receiver_account_creation_fee_paid.then(|| ACCOUNT_CREATION_FEE.to_string()),
      };
      user_commands.push(meta);
    }

    // Internal commands: coinbase + fee transfers + SNARK-work fees (all applied; nanomina).
    //
    // The block producer pays the SNARK-work fees out of its fee pool, so its coinbase/fee
    // credits are reported NET of them, and each prover *other than* the producer is credited
    // its fee (a prover == producer nets out — its fee is forfeited, not re-credited). This
    // matches the ledger effect and the Postgres internal-command behavior.
    let producer = ix.transactions.coinbase_receiver.clone();
    let total_snark: u64 = ix.snark_jobs.iter().map(|j| j.fee).sum();
    // Snark comes out of the producer's own fee transfers first, then its coinbase.
    let producer_fee_total: u64 = ix
      .transactions
      .fee_transfer
      .iter()
      .filter(|ft| producer.as_ref() == Some(&ft.recipient))
      .filter_map(|ft| ft.fee.parse::<u64>().ok())
      .sum();
    let snark_from_fees = total_snark.min(producer_fee_total);
    let snark_from_coinbase = total_snark - snark_from_fees;

    let mut seq = 0i32;
    if ix.transactions.coinbase != "0" {
      if let Some(receiver) = &ix.transactions.coinbase_receiver {
        let coinbase = ix.transactions.coinbase.parse::<u64>().unwrap_or(0).saturating_sub(snark_from_coinbase);
        if coinbase > 0 {
          let meta = InternalCommandMetadata {
            command_type: InternalCommandType::Coinbase,
            receiver: receiver.clone(),
            fee: Some(coinbase.to_string()),
            hash: ix.state_hash.clone(),
            creation_fee: ix
              .transactions
              .coinbase_receiver_account_creation_fee_paid
              .then(|| ACCOUNT_CREATION_FEE.to_string()),
            sequence_no: seq,
            secondary_sequence_no: 0,
            status: TransactionStatus::Applied,
            coinbase_receiver: Some(receiver.clone()),
          };
          internal_commands.push(meta);
          seq += 1;
        }
      }
    }
    let mut fee_deduct_remaining = snark_from_fees;
    for ft in &ix.transactions.fee_transfer {
      let mut fee = ft.fee.parse::<u64>().unwrap_or(0);
      if producer.as_ref() == Some(&ft.recipient) && fee_deduct_remaining > 0 {
        let d = fee.min(fee_deduct_remaining);
        fee -= d;
        fee_deduct_remaining -= d;
      }
      if fee == 0 {
        continue;
      }
      // Always plain: the producer's debit for snark/via-coinbase fees is already modeled by
      // netting `total_snark` out of the producer above — classifying as via-coinbase here
      // would debit the producer a second time.
      let meta = InternalCommandMetadata {
        command_type: InternalCommandType::FeeTransfer,
        receiver: ft.recipient.clone(),
        fee: Some(fee.to_string()),
        hash: ix.state_hash.clone(),
        creation_fee: None,
        sequence_no: seq,
        secondary_sequence_no: 0,
        status: TransactionStatus::Applied,
        coinbase_receiver: producer.clone(),
      };
      internal_commands.push(meta);
      seq += 1;
    }

    Ok(ArchiveBlock {
      block_identifier,
      parent_block_identifier,
      timestamp,
      creator: Some(ix.creator_account.public_key.clone()),
      user_commands,
      internal_commands,
      // The indexer does not itemize zkApp commands on this branch.
      zkapp_commands: Vec::new(),
    })
  }

  /// Historical balance from the indexer's staged ledger. No vesting `timing_info` is available
  /// there, so — like the light-node path — the whole balance is reported liquid
  /// (`locked_balance: 0`). Token filtering defaults to MINA.
  async fn historical_balance(
    &self,
    public_key: &str,
    metadata: Option<Value>,
    partial: &PartialBlockIdentifier,
  ) -> Result<ArchiveAccountBalance, MinaMeshError> {
    let token_id = Wrapper(metadata).token_id_or_default()?;
    let block = self.resolve_block(partial).await?;
    let block_identifier = BlockIdentifier { hash: block.state_hash.clone(), index: block.block_height as i64 };
    match self.client.staged_account(public_key, block.block_height, None).await? {
      Some(acct) => Ok(ArchiveAccountBalance::Found {
        block_identifier,
        token_id,
        total_balance: acct.balance_nano,
        liquid_balance: acct.balance_nano,
        locked_balance: 0,
        nonce: acct.nonce as u64,
      }),
      // The indexer holds the staged ledger at that block, so not finding the account means it
      // does not exist there.
      None => Ok(ArchiveAccountBalance::Absent { block_identifier }),
    }
  }

  /// Search over the trustless indexer. The indexer has no offset/cursor pagination, no
  /// `total_count`, and no combined sender-OR-receiver filter, so we fetch sender and receiver
  /// user commands separately, union + dedupe by hash, filter, and page in Rust. Degradations
  /// vs Postgres: only user commands (no internal/zkApp commands in search); `total_count` is
  /// the size of the fetched window (capped), not the global total.
  async fn search_transactions(
    &self,
    req: &SearchTransactionsRequest,
  ) -> Result<ArchiveTransactionPage, MinaMeshError> {
    let qp = SearchTransactionsQueryParams::try_from(req.clone())?;
    let limit = req.limit.unwrap_or(100).max(0) as usize;
    let offset = req.offset.unwrap_or(0).max(0) as usize;
    let max_height = qp.max_block.map(|h| h as u32);

    let mut txns: Vec<IxSearchTxn> = Vec::new();
    if let Some(hash) = &qp.transaction_hash {
      if let Some(t) = self.client.transaction_by_hash(hash).await? {
        txns.push(t);
      }
    } else if let Some(pk) = qp.account_identifier.clone().or_else(|| qp.address.clone()) {
      // Fetch enough rows to cover the requested page; this also bounds total_count.
      let cap = offset + limit.max(1) + 50;
      let outgoing = self.client.account_transactions(&pk, true, max_height, cap).await?;
      let incoming = self.client.account_transactions(&pk, false, max_height, cap).await?;
      let mut seen = std::collections::HashSet::new();
      for t in outgoing.into_iter().chain(incoming.into_iter()) {
        if seen.insert(t.hash.clone()) {
          txns.push(t);
        }
      }
      // Newest first, hash as a stable tiebreak.
      txns.sort_by(|a, b| b.block_height.cmp(&a.block_height).then_with(|| a.hash.cmp(&b.hash)));
    }

    // applied/failed filters (both map to is_applied).
    if let Some(status) = &qp.status {
      let want_applied = matches!(status, TransactionStatus::Applied);
      txns.retain(|t| t.is_applied == want_applied);
    }
    if let Some(success) = &qp.success_status {
      let want_applied = matches!(success, TransactionStatus::Applied);
      txns.retain(|t| t.is_applied == want_applied);
    }

    let total_count = txns.len() as i64;
    let commands: Vec<ArchiveSearchCommand> = txns.iter().skip(offset).take(limit).map(ix_search_to_command).collect();
    let next_offset = offset as i64 + commands.len() as i64;
    Ok(ArchiveTransactionPage {
      commands,
      // The indexer's search covers user commands only.
      zkapp_commands: Vec::new(),
      total_count,
      next_offset: if next_offset < total_count { Some(next_offset) } else { None },
    })
  }

  async fn account_nonce(&self, public_key: &str) -> Result<Option<u32>, MinaMeshError> {
    self.client.account_nonce(public_key).await
  }

  async fn payment_in_history(&self, payment: &Payment) -> Result<PaymentHistory, MinaMeshError> {
    let sender = &payment.from;
    let receiver = &payment.to;
    let nonce = payment.nonce as i64;
    // Scan the sender's recent commands for an exact duplicate.
    const SCAN: usize = 200;
    let txns = self.client.account_transactions(sender, true, None, SCAN).await?;
    let found = txns.iter().any(|t| {
      t.nonce as i64 == nonce
        && t.amount == payment.amount
        && t.fee == payment.fee
        && t.to.as_deref() == Some(receiver.as_str())
    });
    if found {
      return Ok(PaymentHistory::Applied);
    }
    // The scan is bounded, so a full page means older commands were not looked at and this
    // payment could be among them. Saying `Absent` there is a false negative, and a false
    // negative here is a duplicate submission.
    if txns.len() >= SCAN {
      return Ok(PaymentHistory::Unknown);
    }
    Ok(PaymentHistory::Absent)
  }
}

// ===========================================================================================
// PostgresArchive — raw Mina archive Postgres adapter (the rosetta SQL). TrustedArchive.
// ===========================================================================================

/// Trusted history adapter over a Mina archive Postgres. This is the original rosetta-derived
/// SQL path: multi-table joins for blocks/commands and staged-ledger balance reconstruction
/// (including vesting `timing_info`). `provenance() == TrustedArchive`.
#[derive(Debug)]
pub struct PostgresArchive {
  pool: PgPool,
  search_tx_optimized: bool,
}

impl PostgresArchive {
  pub fn new(pool: PgPool, search_tx_optimized: bool) -> Self {
    Self { pool, search_tx_optimized }
  }

  async fn user_commands(&self, metadata: &BlockMetadata) -> Result<Vec<UserCommandMetadata>, MinaMeshError> {
    Ok(
      sqlx::query_file_as!(UserCommandMetadata, "sql/queries/user_commands.sql", metadata.id)
        .fetch_all(&self.pool)
        .await?,
    )
  }

  async fn internal_commands(&self, metadata: &BlockMetadata) -> Result<Vec<InternalCommandMetadata>, MinaMeshError> {
    Ok(
      sqlx::query_file_as!(InternalCommandMetadata, "sql/queries/internal_commands.sql", metadata.id, DEFAULT_TOKEN_ID)
        .fetch_all(&self.pool)
        .await?,
    )
  }

  async fn zkapp_commands(&self, metadata: &BlockMetadata) -> Result<Vec<ZkAppCommand>, MinaMeshError> {
    Ok(
      sqlx::query_file_as!(ZkAppCommand, "sql/queries/zkapp_commands.sql", metadata.id, DEFAULT_TOKEN_ID)
        .fetch_all(&self.pool)
        .await?,
    )
  }

  async fn block_metadata(
    &self,
    PartialBlockIdentifier { index, hash }: &PartialBlockIdentifier,
  ) -> Result<Option<BlockMetadata>, MinaMeshError> {
    let pool = &self.pool;
    let metadata = if let (Some(index), Some(hash)) = (&index, &hash) {
      sqlx::query_file_as!(BlockMetadata, "sql/queries/query_both.sql", hash.to_string(), index)
        .fetch_optional(pool)
        .await?
    } else if let Some(index) = index {
      let record = sqlx::query_file!("sql/queries/max_canonical_height.sql").fetch_one(pool).await?;
      if index <= &record.max_canonical_height.unwrap() {
        sqlx::query_file_as!(BlockMetadata, "sql/queries/query_canonical.sql", index).fetch_optional(pool).await?
      } else {
        sqlx::query_file_as!(BlockMetadata, "sql/queries/query_pending.sql", index).fetch_optional(pool).await?
      }
    } else if let Some(hash) = &hash {
      sqlx::query_file_as!(BlockMetadata, "sql/queries/query_hash.sql", hash).fetch_optional(pool).await?
    } else {
      sqlx::query_file_as!(BlockMetadata, "sql/queries/query_best.sql").fetch_optional(pool).await?
    };
    Ok(metadata)
  }

  async fn fetch_user_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<UserCommand>, MinaMeshError> {
    // Two literals because `query_file_as!` needs a compile-time path.
    let user_commands = if !self.search_tx_optimized {
      sqlx::query_file_as!(
        UserCommand,
        "sql/queries/indexer_user_commands.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset,
      )
      .fetch_all(&self.pool)
      .await?
    } else {
      sqlx::query_file_as!(
        UserCommand,
        "sql/queries/indexer_user_commands_optimized.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset,
      )
      .fetch_all(&self.pool)
      .await?
    };
    Ok(user_commands)
  }

  async fn fetch_internal_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<InternalCommand>, MinaMeshError> {
    let internal_commands = if !self.search_tx_optimized {
      sqlx::query_file_as!(
        InternalCommand,
        "sql/queries/indexer_internal_commands.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset
      )
      .fetch_all(&self.pool)
      .await?
    } else {
      sqlx::query_file_as!(
        InternalCommand,
        "sql/queries/indexer_internal_commands_optimized.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset
      )
      .fetch_all(&self.pool)
      .await?
    };
    Ok(internal_commands)
  }

  async fn fetch_zkapp_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<ZkAppCommand>, MinaMeshError> {
    let zkapp_commands = if !self.search_tx_optimized {
      sqlx::query_file_as!(
        ZkAppCommand,
        "sql/queries/indexer_zkapp_commands.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset
      )
      .fetch_all(&self.pool)
      .await?
    } else {
      sqlx::query_file_as!(
        ZkAppCommand,
        "sql/queries/indexer_zkapp_commands_optimized.sql",
        query_params.max_block,
        query_params.transaction_hash,
        query_params.account_identifier,
        query_params.token_id,
        query_params.status.clone() as Option<TransactionStatus>,
        query_params.success_status.clone() as Option<TransactionStatus>,
        query_params.address,
        limit,
        offset
      )
      .fetch_all(&self.pool)
      .await?
    };
    Ok(zkapp_commands)
  }
}

#[async_trait]
impl MinaArchive for PostgresArchive {
  fn provenance(&self) -> Provenance {
    Provenance::TrustedArchive
  }

  async fn tip(&self) -> Result<ArchiveTip, MinaMeshError> {
    let best = self
      .block_metadata(&PartialBlockIdentifier { index: None, hash: None })
      .await?
      .ok_or(MinaMeshError::ChainInfoMissing)?;
    Ok(ArchiveTip {
      block_identifier: BlockIdentifier::new(best.height, best.state_hash),
      timestamp: best.timestamp.parse::<i64>()?,
    })
  }

  async fn oldest_block_identifier(&self) -> Result<BlockIdentifier, MinaMeshError> {
    let oldest_block = sqlx::query_file!("sql/queries/oldest_block.sql").fetch_one(&self.pool).await?;
    Ok(BlockIdentifier::new(oldest_block.height, oldest_block.state_hash))
  }

  async fn block(&self, partial: &PartialBlockIdentifier) -> Result<ArchiveBlock, MinaMeshError> {
    let metadata = match self.block_metadata(partial).await? {
      Some(metadata) => metadata,
      None => return Err(MinaMeshError::BlockMissing(partial.index, partial.hash.clone())),
    };
    let parent_block_metadata = match &metadata.parent_id {
      Some(parent_id) => {
        sqlx::query_file_as!(BlockMetadata, "sql/queries/query_id.sql", parent_id).fetch_optional(&self.pool).await?
      }
      None => None,
    };
    let block_identifier = BlockIdentifier::new(metadata.height, metadata.state_hash.clone());
    let parent_block_identifier = match parent_block_metadata {
      Some(block_metadata) => BlockIdentifier::new(block_metadata.height, block_metadata.state_hash),
      None => block_identifier.clone(),
    };
    let (user_commands, internal_commands, zkapp_commands) = tokio::try_join!(
      self.user_commands(&metadata),
      self.internal_commands(&metadata),
      self.zkapp_commands(&metadata)
    )?;

    Ok(ArchiveBlock {
      block_identifier,
      parent_block_identifier,
      timestamp: metadata.timestamp.parse()?,
      creator: Some(metadata.creator),
      user_commands,
      internal_commands,
      zkapp_commands,
    })
  }

  async fn historical_balance(
    &self,
    public_key: &str,
    metadata: Option<Value>,
    partial: &PartialBlockIdentifier,
  ) -> Result<ArchiveAccountBalance, MinaMeshError> {
    let index = partial.index;
    let hash = partial.hash.clone();
    let block = sqlx::query_file!("sql/queries/maybe_block.sql", index, hash)
      .fetch_optional(&self.pool)
      .await?
      .ok_or(MinaMeshError::BlockMissing(index, hash.clone()))?;
    let maybe_account_balance_info = sqlx::query_file!(
      "sql/queries/maybe_account_balance_info.sql",
      public_key,
      block.height.ok_or(MinaMeshError::ChainInfoMissing)?,
      Wrapper(metadata).token_id_or_default()?
    )
    .fetch_optional(&self.pool)
    .await?;
    match maybe_account_balance_info {
      // The archive holds all history, so no row means the account does not exist at that block.
      None => Ok(ArchiveAccountBalance::Absent {
        block_identifier: build_block_identifier(block.height, block.state_hash, index, hash)?,
      }),
      Some(account_balance_info) => {
        let token_id = account_balance_info.token_id;
        let nonce = account_balance_info.nonce;
        let last_relevant_command_balance = account_balance_info.balance.parse::<u64>()?;
        let timing_info = sqlx::query_file!("sql/queries/timing_info.sql", account_balance_info.timing_id)
          .fetch_optional(&self.pool)
          .await?;
        let liquid_balance = match timing_info {
          Some(timing_info) => {
            let incremental_balance = incremental_balance_between_slots(
              account_balance_info.block_global_slot_since_genesis.ok_or(MinaMeshError::ChainInfoMissing)? as u32,
              block.global_slot_since_genesis.ok_or(MinaMeshError::ChainInfoMissing)? as u32,
              timing_info.cliff_time as u32,
              timing_info.cliff_amount.parse::<u64>()?,
              timing_info.vesting_period as u32,
              timing_info.vesting_increment.parse::<u64>()?,
              timing_info.initial_minimum_balance.parse::<u64>()?,
            );
            last_relevant_command_balance + incremental_balance
          }
          None => last_relevant_command_balance,
        };
        let total_balance = last_relevant_command_balance;
        let locked_balance = total_balance - liquid_balance;
        Ok(ArchiveAccountBalance::Found {
          block_identifier: build_block_identifier(block.height, block.state_hash, index, hash)?,
          token_id,
          total_balance,
          liquid_balance,
          locked_balance,
          nonce: nonce as u64,
        })
      }
    }
  }

  async fn search_transactions(
    &self,
    req: &SearchTransactionsRequest,
  ) -> Result<ArchiveTransactionPage, MinaMeshError> {
    let original_offset = req.offset.unwrap_or(0);
    let mut offset = original_offset;
    let mut limit = req.limit.unwrap_or(100);
    let mut commands: Vec<ArchiveSearchCommand> = Vec::new();
    let mut zkapp_rows: Vec<ZkAppCommand> = Vec::new();
    let mut total_count = 0;

    let query_params = SearchTransactionsQueryParams::try_from(req.clone())?;

    // User Commands
    let user_commands = self.fetch_user_commands(&query_params, offset, limit).await?;
    let user_commands_total_count = user_commands.first().and_then(|uc| uc.total_count).unwrap_or(0);
    commands.extend(user_commands.into_iter().map(ArchiveSearchCommand::from));
    total_count += user_commands_total_count;

    // Internal Commands
    let mut internal_commands_bt_len = 0;
    if limit > commands.len() as i64 {
      // if we are below the limit, fetch internal commands
      (offset, limit) = adjust_limit_and_offset(limit, offset, commands.len() as i64);
      let internal_commands = self.fetch_internal_commands(&query_params, offset, limit).await?;
      let internal_commands_total_count = internal_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      let internal_commands_bt: Vec<ArchiveSearchCommand> =
        internal_commands.into_iter().map(ArchiveSearchCommand::from).collect();
      internal_commands_bt_len = internal_commands_bt.len();
      commands.extend(internal_commands_bt);
      total_count += internal_commands_total_count;
    } else {
      // otherwise only fetch the first internal command to get the total count
      let internal_commands = self.fetch_internal_commands(&query_params, 0, 1).await?;
      let internal_commands_total_count = internal_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      total_count += internal_commands_total_count;
    }

    // ZkApp Commands
    if limit > commands.len() as i64 {
      // if we are below the limit, fetch zkapp commands
      (offset, limit) = adjust_limit_and_offset(limit, offset, internal_commands_bt_len as i64);
      let zkapp_commands = self.fetch_zkapp_commands(&query_params, offset, limit).await?;
      let zkapp_commands_total_count = zkapp_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      zkapp_rows.extend(zkapp_commands);
      total_count += zkapp_commands_total_count;
    } else {
      // otherwise only fetch the first zkapp command to get the total count
      let zkapp_commands = self.fetch_zkapp_commands(&query_params, 0, 1).await?;
      let zkapp_commands_total_count = zkapp_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      total_count += zkapp_commands_total_count;
    }

    let next_offset = original_offset + commands.len() as i64 + zkapp_rows.len() as i64;
    Ok(ArchiveTransactionPage {
      commands,
      zkapp_commands: zkapp_rows,
      total_count,
      next_offset: if next_offset < total_count { Some(next_offset) } else { None },
    })
  }

  async fn account_nonce(&self, _public_key: &str) -> Result<Option<u32>, MinaMeshError> {
    // The raw archive doesn't expose a "current account nonce" the way the indexer does;
    // in full mode `construction/metadata` sources the inferred nonce from the daemon instead
    // (see `construction_metadata`), so this is never reached. Fail loudly if it ever is.
    Err(MinaMeshError::Exception(
      "account_nonce is not available from the Postgres archive; use the daemon (full mode)".to_string(),
    ))
  }

  async fn payment_in_history(&self, payment: &Payment) -> Result<PaymentHistory, MinaMeshError> {
    let sender = &payment.from;
    let receiver = &payment.to;
    let nonce = payment.nonce as i64;
    let amount = &payment.amount.to_string();
    let fee = &payment.fee.to_string();
    let row = sqlx::query_file!("sql/queries/query_payment.sql", nonce, sender, receiver, amount, fee)
      .fetch_optional(&self.pool)
      .await?;
    // A full-history archive can rule a payment out, so there is no `Unknown` case here.
    Ok(if row.is_some() { PaymentHistory::Applied } else { PaymentHistory::Absent })
  }
}

// ===========================================================================================
// Shared history-only assembly helpers (moved here from the handlers).
// ===========================================================================================

/// Block header row read from the archive Postgres.
#[derive(Debug, PartialEq, Eq, FromRow, Serialize)]
pub struct BlockMetadata {
  id: i32,
  block_winner_id: i32,
  chain_status: Option<ChainStatus>,
  creator_id: i32,
  global_slot_since_genesis: i64,
  global_slot_since_hard_fork: i64,
  height: i64,
  last_vrf_output: String,
  ledger_hash: String,
  min_window_density: i64,
  next_epoch_data_id: i32,
  state_hash: String,
  sub_window_densities: Vec<i64>,
  timestamp: String,
  total_currency: Option<String>,
  parent_hash: String,
  parent_id: Option<i32>,
  proposed_protocol_version_id: Option<i32>,
  protocol_version_id: i32,
  snarked_ledger_hash_id: i32,
  staking_epoch_data_id: i32,
  creator: String,
  winner: String,
}

/// Build a Rosetta transaction for one internal command (coinbase / fee transfer).
pub(crate) fn internal_command_transaction(meta: &InternalCommandMetadata) -> Transaction {
  let id = generate_internal_command_transaction_identifier(
    &meta.command_type,
    meta.sequence_no,
    meta.secondary_sequence_no,
    &meta.hash,
  );
  Transaction::new(TransactionIdentifier::new(id), generate_operations_internal_command(meta))
}

pub fn zkapp_commands_to_transactions(commands: Vec<ZkAppCommand>) -> Vec<Transaction> {
  let block_map = generate_operations_zkapp_command(commands);

  let mut result = Vec::new();
  for (_, tx_map) in block_map {
    for (tx_hash, operations) in tx_map {
      result.push(Transaction {
        transaction_identifier: Box::new(TransactionIdentifier { hash: tx_hash }),
        operations,
        metadata: None,
        related_transactions: None,
      });
    }
  }
  result
}

pub fn zkapp_commands_to_block_transactions(
  commands: Vec<ZkAppCommand>,
  include_timestamp: bool,
) -> Vec<BlockTransaction> {
  let block_map = generate_operations_zkapp_command(commands);

  let mut result = Vec::new();
  for ((block_index, block_hash, timestamp), tx_map) in block_map {
    let block_index = block_index.unwrap_or(0);
    let block_hash = block_hash.unwrap_or_default();
    for (tx_hash, operations) in tx_map {
      result.push(BlockTransaction {
        block_identifier: Box::new(BlockIdentifier { index: block_index, hash: block_hash.clone() }),
        transaction: Box::new(Transaction {
          transaction_identifier: Box::new(TransactionIdentifier { hash: tx_hash }),
          operations,
          metadata: None,
          related_transactions: None,
        }),
        timestamp: if include_timestamp {
          Some(timestamp.clone().unwrap_or_default().parse::<i64>().unwrap_or_default())
        } else {
          None
        },
      });
    }
  }
  result
}

impl From<InternalCommand> for BlockTransaction {
  fn from(internal_command: InternalCommand) -> Self {
    let transaction_identifier = generate_internal_command_transaction_identifier(
      &internal_command.command_type,
      internal_command.sequence_no,
      internal_command.secondary_sequence_no,
      &internal_command.hash,
    );
    let operations = generate_operations_internal_command(&internal_command);
    let block_identifier = BlockIdentifier::new(
      internal_command.height.unwrap_or_default(),
      internal_command.state_hash.unwrap_or_default(),
    );
    let transaction = Transaction {
      transaction_identifier: Box::new(TransactionIdentifier::new(transaction_identifier)),
      operations,
      related_transactions: None,
      metadata: None,
    };
    BlockTransaction::new(block_identifier, transaction)
  }
}

impl From<UserCommand> for ArchiveSearchCommand {
  fn from(command: UserCommand) -> Self {
    let timestamp = command.timestamp.as_ref().and_then(|ts| ts.parse::<i64>().ok());
    let block_identifier =
      BlockIdentifier::new(command.height.unwrap_or_default(), command.state_hash.clone().unwrap_or_default());
    ArchiveSearchCommand {
      block_identifier,
      timestamp,
      command: ArchiveSearchCommandKind::User(UserCommandMetadata {
        command_type: command.command_type,
        nonce: command.nonce,
        amount: command.amount,
        fee: command.fee,
        valid_until: command.valid_until,
        memo: command.memo,
        hash: command.hash,
        fee_payer: command.fee_payer,
        source: command.source,
        receiver: command.receiver,
        status: command.status,
        failure_reason: command.failure_reason,
        creation_fee: command.creation_fee,
      }),
    }
  }
}

impl From<InternalCommand> for ArchiveSearchCommand {
  fn from(command: InternalCommand) -> Self {
    let timestamp = command.timestamp.as_ref().and_then(|ts| ts.parse::<i64>().ok());
    let block_identifier =
      BlockIdentifier::new(command.height.unwrap_or_default(), command.state_hash.clone().unwrap_or_default());
    ArchiveSearchCommand {
      block_identifier,
      timestamp,
      command: ArchiveSearchCommandKind::Internal(InternalCommandMetadata {
        command_type: command.command_type,
        receiver: command.receiver,
        fee: command.fee,
        hash: command.hash,
        creation_fee: command.creation_fee,
        sequence_no: command.sequence_no,
        secondary_sequence_no: command.secondary_sequence_no,
        status: command.status,
        coinbase_receiver: command.coinbase_receiver,
      }),
    }
  }
}

impl From<UserCommand> for BlockTransaction {
  fn from(user_command: UserCommand) -> Self {
    let metadata = generate_transaction_metadata(&user_command);
    let operations = generate_operations_user_command(&user_command);
    let block_identifier =
      BlockIdentifier::new(user_command.height.unwrap_or_default(), user_command.state_hash.unwrap_or_default());
    let transaction = Transaction {
      transaction_identifier: Box::new(TransactionIdentifier::new(user_command.hash)),
      operations,
      metadata,
      related_transactions: None,
    };
    BlockTransaction::new(block_identifier, transaction)
  }
}

/// Map an indexer user-command search row into a Rosetta `BlockTransaction`, reusing the
/// shared operation generators via `UserCommandMetadata`.
fn ix_search_to_command(tx: &IxSearchTxn) -> ArchiveSearchCommand {
  let command_type =
    if tx.kind.to_uppercase().contains("DELEG") { UserCommandType::Delegation } else { UserCommandType::Payment };
  let amount = match command_type {
    UserCommandType::Payment => Some(tx.amount.to_string()),
    UserCommandType::Delegation => None,
  };
  let meta = UserCommandMetadata {
    command_type,
    nonce: tx.nonce as i64,
    amount,
    fee: Some(tx.fee.to_string()),
    valid_until: None,
    memo: Some(tx.memo.clone()),
    hash: tx.hash.clone(),
    fee_payer: tx.from.clone(),
    source: tx.from.clone(),
    receiver: tx.to.clone().unwrap_or_default(),
    status: if tx.is_applied { TransactionStatus::Applied } else { TransactionStatus::Failed },
    failure_reason: tx.failure_reason.clone(),
    creation_fee: tx.receiver_account_creation_fee_paid.then(|| ACCOUNT_CREATION_FEE.to_string()),
  };
  ArchiveSearchCommand {
    block_identifier: BlockIdentifier::new(tx.block_height as i64, tx.block.state_hash.clone()),
    // The indexer exposes ISO datetimes, not the epoch millis Rosetta wants, so this parses
    // only when the value happens to be numeric.
    timestamp: tx.block.date_time.parse::<i64>().ok(),
    command: ArchiveSearchCommandKind::User(meta),
  }
}

pub struct SearchTransactionsQueryParams {
  pub max_block: Option<i64>,
  pub transaction_hash: Option<String>,
  pub account_identifier: Option<String>,
  pub token_id: Option<String>,
  pub status: Option<TransactionStatus>,
  pub success_status: Option<TransactionStatus>,
  pub address: Option<String>,
}

impl TryFrom<SearchTransactionsRequest> for SearchTransactionsQueryParams {
  type Error = MinaMeshError;

  fn try_from(req: SearchTransactionsRequest) -> Result<Self, Self::Error> {
    let max_block = req.max_block;
    let transaction_hash = req.transaction_identifier.map(|t| t.hash);
    // token_id can be found in the metadata of the account_identifier
    let token_id = req
      .account_identifier
      .as_ref()
      .and_then(|a| a.metadata.as_ref())
      .and_then(|m| m.get("token_id"))
      .map(|t| t.as_str().unwrap().to_string());
    let account_identifier = req.account_identifier.map(|a| a.address);

    let status = match req.status.as_deref() {
      Some("applied") => Some(TransactionStatus::Applied),
      Some("failed") => Some(TransactionStatus::Failed),
      Some(other) => {
        return Err(MinaMeshError::Exception(format!(
          "Invalid transaction status: '{}'. Valid statuses are 'applied' and 'failed'",
          other
        )));
      }
      None => None,
    };

    let success_status = match req.success {
      Some(true) => Some(TransactionStatus::Applied),
      Some(false) => Some(TransactionStatus::Failed),
      None => None,
    };

    Ok(SearchTransactionsQueryParams {
      max_block,
      transaction_hash,
      account_identifier,
      token_id,
      status,
      success_status,
      address: req.address,
    })
  }
}

fn adjust_limit_and_offset(mut limit: i64, mut offset: i64, txs_len: i64) -> (i64, i64) {
  if offset >= txs_len {
    offset -= txs_len;
  } else {
    offset = 0;
  }
  if limit >= txs_len {
    limit -= txs_len;
  } else {
    limit = 0;
  }
  (offset, limit)
}

fn min_balance_at_slot(
  global_slot: u32,
  cliff_time: u32,
  cliff_amount: u64,
  vesting_period: u32,
  vesting_increment: u64,
  initial_minimum_balance: u64,
) -> u64 {
  if global_slot < cliff_time {
    initial_minimum_balance
  } else if vesting_period == 0 {
    0
  } else {
    let min_balance_past_cliff = initial_minimum_balance.saturating_sub(cliff_amount);
    if min_balance_past_cliff == 0 {
      0
    } else {
      let num_periods = (global_slot - cliff_time) / vesting_period;
      let vesting_decrement = if (u64::MAX / num_periods as u64) < vesting_increment {
        u64::MAX
      } else {
        num_periods as u64 * vesting_increment
      };
      min_balance_past_cliff.saturating_sub(vesting_decrement)
    }
  }
}

fn incremental_balance_between_slots(
  start_slot: u32,
  end_slot: u32,
  cliff_time: u32,
  cliff_amount: u64,
  vesting_period: u32,
  vesting_increment: u64,
  initial_minimum_balance: u64,
) -> u64 {
  if end_slot <= start_slot {
    return 0;
  }
  let min_balance_at_start_slot = min_balance_at_slot(
    start_slot,
    cliff_time,
    cliff_amount,
    vesting_period,
    vesting_increment,
    initial_minimum_balance,
  );
  let min_balance_at_end_slot =
    min_balance_at_slot(end_slot, cliff_time, cliff_amount, vesting_period, vesting_increment, initial_minimum_balance);
  min_balance_at_start_slot.saturating_sub(min_balance_at_end_slot)
}

fn build_block_identifier(
  db_height: Option<i64>,
  db_hash: Option<String>,
  index: Option<i64>,
  hash: Option<String>,
) -> Result<BlockIdentifier, MinaMeshError> {
  Ok(BlockIdentifier {
    hash: db_hash.clone().ok_or(MinaMeshError::BlockMissing(index, hash.clone()))?,
    index: db_height.ok_or(MinaMeshError::BlockMissing(index, hash))?,
  })
}

#[cfg(test)]
mod balance_assembly_tests {
  use coinbase_mesh::models::{AccountBalanceResponse, BlockIdentifier};

  use super::ArchiveAccountBalance;
  use crate::util::DEFAULT_TOKEN_ID;

  fn balance(token_id: &str, total: u64, liquid: u64, locked: u64) -> ArchiveAccountBalance {
    ArchiveAccountBalance::Found {
      block_identifier: BlockIdentifier::new(42, "3NLa".to_string()),
      token_id: token_id.to_string(),
      total_balance: total,
      liquid_balance: liquid,
      locked_balance: locked,
      nonce: 7,
    }
  }

  /// An account that does not exist reports zero, which is what Rosetta expects — but it is now
  /// a case an adapter has to choose, not the fallback for anything it failed to find.
  ///
  /// The response must be exactly what the previous not-found path produced, including the
  /// metadata: this change is about what an adapter is allowed to claim, not about the wire.
  #[test]
  fn an_absent_account_reports_zero_exactly_as_before() {
    let response: AccountBalanceResponse =
      ArchiveAccountBalance::Absent { block_identifier: BlockIdentifier::new(42, "3NLa".to_string()) }.into();
    let amount = &response.balances[0];
    assert_eq!(amount.value, "0");
    assert_eq!(amount.currency.symbol, "MINA");
    let metadata = amount.metadata.as_ref().expect("the split metadata is part of the response");
    assert_eq!(metadata["total_balance"], 0);
    assert_eq!(metadata["liquid_balance"], 0);
    assert_eq!(metadata["locked_balance"], 0);
    assert_eq!(response.metadata.as_ref().unwrap()["nonce"], "0");
    assert_eq!(response.metadata.as_ref().unwrap()["created_via_historical_lookup"], true);
  }

  // Rosetta's `value` is the spendable balance; the split rides in metadata.
  #[test]
  fn value_is_the_liquid_balance_and_the_split_is_metadata() {
    let response: AccountBalanceResponse = balance(DEFAULT_TOKEN_ID, 1000, 600, 400).into();
    let amount = &response.balances[0];
    assert_eq!(amount.value, "600");
    let metadata = amount.metadata.as_ref().unwrap();
    assert_eq!(metadata["total_balance"], 1000);
    assert_eq!(metadata["liquid_balance"], 600);
    assert_eq!(metadata["locked_balance"], 400);
  }

  #[test]
  fn the_nonce_and_block_are_carried_through() {
    let response: AccountBalanceResponse = balance(DEFAULT_TOKEN_ID, 1, 1, 0).into();
    assert_eq!(response.block_identifier.index, 42);
    assert_eq!(response.block_identifier.hash, "3NLa");
    let metadata = response.metadata.as_ref().unwrap();
    assert_eq!(metadata["nonce"], "7");
    assert_eq!(metadata["created_via_historical_lookup"], true);
  }

  // An account absent at that block reports a default-token zero rather than naming a token it
  // was never found holding.
  #[test]
  fn a_custom_token_is_named_in_the_currency() {
    let response: AccountBalanceResponse =
      balance("wTYTc38ab19XT4oPPv7pajgGEUWWXc5AzDKvqqCNiFBfLCCnXK", 5, 5, 0).into();
    assert_ne!(response.balances[0].currency.symbol, "MINA");
  }
}

#[cfg(test)]
mod search_assembly_tests {
  use coinbase_mesh::models::BlockIdentifier;

  use super::{ArchiveSearchCommand, ArchiveSearchCommandKind, ArchiveTransactionPage};
  use crate::{InternalCommandMetadata, InternalCommandType, TransactionStatus, UserCommandMetadata, UserCommandType};

  fn user_hit(hash: &str, timestamp: Option<i64>) -> ArchiveSearchCommand {
    ArchiveSearchCommand {
      block_identifier: BlockIdentifier::new(42, "3NLa".to_string()),
      timestamp,
      command: ArchiveSearchCommandKind::User(UserCommandMetadata {
        command_type: UserCommandType::Payment,
        nonce: 1,
        amount: Some("2000000000".to_string()),
        fee: Some("100000000".to_string()),
        valid_until: None,
        memo: None,
        hash: hash.to_string(),
        fee_payer: "B62qsender".to_string(),
        source: "B62qsender".to_string(),
        receiver: "B62qreceiver".to_string(),
        status: TransactionStatus::Applied,
        failure_reason: None,
        creation_fee: None,
      }),
    }
  }

  fn internal_hit() -> ArchiveSearchCommand {
    ArchiveSearchCommand {
      block_identifier: BlockIdentifier::new(42, "3NLa".to_string()),
      timestamp: Some(1_700_000_000_000),
      command: ArchiveSearchCommandKind::Internal(InternalCommandMetadata {
        command_type: InternalCommandType::Coinbase,
        receiver: "B62qproducer".to_string(),
        fee: Some("720000000000".to_string()),
        hash: "3NLa".to_string(),
        creation_fee: None,
        sequence_no: 0,
        secondary_sequence_no: 0,
        status: TransactionStatus::Applied,
        coinbase_receiver: None,
      }),
    }
  }

  fn page(commands: Vec<ArchiveSearchCommand>) -> ArchiveTransactionPage {
    ArchiveTransactionPage { commands, zkapp_commands: Vec::new(), total_count: 7, next_offset: Some(2) }
  }

  #[test]
  fn hits_become_block_transactions_carrying_their_block() {
    let response = page(vec![user_hit("5Jtx", None), internal_hit()]).into_response(false);
    assert_eq!(response.transactions.len(), 2);
    assert!(response.transactions.iter().all(|bt| bt.block_identifier.index == 42));
    assert_eq!(response.transactions[0].transaction.transaction_identifier.hash, "5Jtx");
    // Internal commands get a synthesized identifier, not a bare hash.
    assert!(response.transactions[1].transaction.transaction_identifier.hash.starts_with("coinbase:"));
  }

  // include_timestamp is a property of the request, so it is applied here rather than by each
  // adapter -- which is the point of moving assembly above the trait.
  #[test]
  fn timestamps_appear_only_when_the_request_asked_for_them() {
    let with = page(vec![user_hit("5Jtx", Some(1_700_000_000_000))]).into_response(true);
    assert_eq!(with.transactions[0].timestamp, Some(1_700_000_000_000));

    let without = page(vec![user_hit("5Jtx", Some(1_700_000_000_000))]).into_response(false);
    assert_eq!(without.transactions[0].timestamp, None);
  }

  #[test]
  fn a_hit_with_no_timestamp_stays_none_even_when_requested() {
    let response = page(vec![user_hit("5Jtx", None)]).into_response(true);
    assert_eq!(response.transactions[0].timestamp, None);
  }

  #[test]
  fn paging_fields_pass_through() {
    let response = page(vec![user_hit("5Jtx", None)]).into_response(false);
    assert_eq!(response.total_count, 7);
    assert_eq!(response.next_offset, Some(2));
  }
}
