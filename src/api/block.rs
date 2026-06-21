use anyhow::Result;
use coinbase_mesh::models::{
  Block, BlockIdentifier, BlockRequest, BlockResponse, PartialBlockIdentifier, Transaction, TransactionIdentifier,
};
use serde::Serialize;
use serde_json::json;
use sqlx::FromRow;

use crate::{
  generate_internal_command_transaction_identifier, generate_operations_internal_command,
  generate_operations_user_command, generate_operations_zkapp_command, generate_transaction_metadata,
  util::DEFAULT_TOKEN_ID, ChainStatus, InternalCommandMetadata, InternalCommandType, MinaMesh, MinaMeshError,
  TransactionStatus, UserCommandMetadata, UserCommandType, ZkAppCommand,
};

/// The Mina account-creation fee (nanomina) — a protocol constant (1 MINA) on these networks.
const ACCOUNT_CREATION_FEE: u64 = 1_000_000_000;

/// https://github.com/MinaProtocol/mina/blob/985eda49bdfabc046ef9001d3c406e688bc7ec45/src/app/rosetta/lib/block.ml#L7
impl MinaMesh {
  pub async fn block(&self, request: BlockRequest) -> Result<BlockResponse, MinaMeshError> {
    self.validate_network(&request.network_identifier).await?;
    let partial_block_identifier = *request.block_identifier;
    // Trustless backend: serve the block + its commands from the indexer.
    if let Some(indexer) = &self.indexer {
      return self.block_from_indexer(indexer, &partial_block_identifier).await;
    }
    let metadata = match self.block_metadata(&partial_block_identifier).await? {
      Some(metadata) => metadata,
      None => return Err(MinaMeshError::BlockMissing(partial_block_identifier.index, partial_block_identifier.hash)),
    };
    let parent_block_metadata = match &metadata.parent_id {
      Some(parent_id) => {
        sqlx::query_file_as!(BlockMetadata, "sql/queries/query_id.sql", parent_id).fetch_optional(self.pg()?).await?
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

    let all_commands: Vec<_> =
      internal_commands.into_iter().chain(user_commands.into_iter()).chain(zkapp_commands.into_iter()).collect();

    Ok(BlockResponse {
      block: Some(Box::new(Block {
        block_identifier: Box::new(block_identifier),
        parent_block_identifier: Box::new(parent_block_identifier),
        timestamp: metadata.timestamp.parse()?,
        transactions: all_commands,
        metadata: Some(json!({ "creator": metadata.creator })),
      })),
      other_transactions: None,
    })
  }

  /// Build a Rosetta block from the trustless indexer. Reuses the same operation
  /// generators as the Postgres path by mapping the indexer's block into the
  /// `UserCommandMetadata` / `InternalCommandMetadata` shapes. Degradations vs Postgres:
  /// no account-creation-fee attribution (`creation_fee: None`); internal-command
  /// transaction identifiers are synthesized from the block hash (the indexer doesn't
  /// expose internal-command hashes); zkApp commands are not itemized.
  async fn block_from_indexer(
    &self,
    indexer: &crate::IndexerClient,
    partial: &PartialBlockIdentifier,
  ) -> Result<BlockResponse, MinaMeshError> {
    let ix = match (&partial.hash, partial.index) {
      (Some(h), _) => indexer.block(None, Some(h)).await?,
      (None, Some(idx)) => indexer.block(Some(idx), None).await?,
      (None, None) => indexer.block(Some(indexer.tip().await?.block_height as i64), None).await?,
    }
    .ok_or_else(|| MinaMeshError::BlockMissing(partial.index, partial.hash.clone()))?;

    let block_identifier = BlockIdentifier::new(ix.block_height as i64, ix.state_hash.clone());
    // Parent links to the previous state hash at height-1; genesis links to itself.
    let parent_block_identifier = if ix.block_height <= 1 {
      block_identifier.clone()
    } else {
      BlockIdentifier::new(ix.block_height as i64 - 1, ix.protocol_state.previous_state_hash.clone())
    };
    let timestamp: i64 = ix.protocol_state.blockchain_state.utc_date.parse()?;

    let mut transactions: Vec<Transaction> = Vec::new();

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
      transactions.push(Transaction {
        transaction_identifier: Box::new(TransactionIdentifier::new(meta.hash.clone())),
        operations: generate_operations_user_command(&meta),
        metadata: generate_transaction_metadata(&meta),
        related_transactions: None,
      });
    }

    // Internal commands: coinbase + fee transfers + SNARK-work fees (all applied; nanomina).
    //
    // The block producer pays the SNARK-work fees out of its fee pool, so its coinbase/fee
    // credits are reported NET of them, and each prover *other than* the producer is credited
    // its fee (a prover == producer nets out — its fee is forfeited, not re-credited). This
    // matches the ledger effect and the Postgres internal-command behavior.
    let producer = ix.transactions.coinbase_receiver.clone();
    // The producer pays ALL SNARK-work fees out of its fee pool, so its coinbase/fee credits
    // are reported net of them. The provers' own fee transfers are already in the list below
    // (as plain fee transfers crediting the prover), so we model the producer's debit once —
    // by netting total snark out of the producer here — and emit every fee transfer as plain
    // (no via-coinbase producer debit, which would double-count). A self-snark prover has no
    // offsetting transfer, so its fee is simply forfeited from the producer (matches the ledger).
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
          transactions.push(internal_command_transaction(&meta));
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
      transactions.push(internal_command_transaction(&meta));
      seq += 1;
    }

    Ok(BlockResponse {
      block: Some(Box::new(Block {
        block_identifier: Box::new(block_identifier),
        parent_block_identifier: Box::new(parent_block_identifier),
        timestamp,
        transactions,
        metadata: Some(json!({ "creator": ix.creator_account.public_key })),
      })),
      other_transactions: None,
    })
  }

  // TODO: use default token value, check how to best handle this
  pub async fn user_commands(&self, metadata: &BlockMetadata) -> Result<Vec<Transaction>, MinaMeshError> {
    let metadata = sqlx::query_file_as!(UserCommandMetadata, "sql/queries/user_commands.sql", metadata.id)
      .fetch_all(self.pg()?)
      .await?;
    let transactions = metadata
      .into_iter()
      .map(|item| {
        let metadata = generate_transaction_metadata(&item);
        let operations = generate_operations_user_command(&item);

        Transaction {
          transaction_identifier: Box::new(TransactionIdentifier::new(item.hash.clone())),
          operations,
          metadata,
          related_transactions: None,
        }
      })
      .collect();
    Ok(transactions)
  }

  pub async fn internal_commands(&self, metadata: &BlockMetadata) -> Result<Vec<Transaction>, MinaMeshError> {
    let metadata =
      sqlx::query_file_as!(InternalCommandMetadata, "sql/queries/internal_commands.sql", metadata.id, DEFAULT_TOKEN_ID)
        .fetch_all(self.pg()?)
        .await?;

    let transactions = metadata
      .into_iter()
      .map(|item| {
        let transaction_identifier = generate_internal_command_transaction_identifier(
          &item.command_type,
          item.sequence_no,
          item.secondary_sequence_no,
          &item.hash,
        );
        Transaction::new(
          TransactionIdentifier::new(transaction_identifier),
          generate_operations_internal_command(&item),
        )
      })
      .collect();
    Ok(transactions)
  }

  pub async fn zkapp_commands(&self, metadata: &BlockMetadata) -> Result<Vec<Transaction>, MinaMeshError> {
    let zkapp_commands =
      sqlx::query_file_as!(ZkAppCommand, "sql/queries/zkapp_commands.sql", metadata.id, DEFAULT_TOKEN_ID)
        .fetch_all(self.pg()?)
        .await?;
    let transactions = zkapp_commands_to_transactions(zkapp_commands);
    Ok(transactions)
  }

  pub async fn block_metadata(
    &self,
    PartialBlockIdentifier { index, hash }: &PartialBlockIdentifier,
  ) -> Result<Option<BlockMetadata>, MinaMeshError> {
    let pool = self.pg()?;
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
}

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
fn internal_command_transaction(meta: &InternalCommandMetadata) -> Transaction {
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
      let transaction = Transaction {
        transaction_identifier: Box::new(TransactionIdentifier { hash: tx_hash }),
        operations,
        metadata: None,
        related_transactions: None,
      };
      result.push(transaction);
    }
  }

  result
}
