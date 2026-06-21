use coinbase_mesh::models::{
  BlockIdentifier, BlockTransaction, SearchTransactionsRequest, SearchTransactionsResponse, Transaction,
  TransactionIdentifier,
};

use crate::{
  generate_internal_command_transaction_identifier, generate_operations_internal_command,
  generate_operations_user_command, generate_operations_zkapp_command, generate_transaction_metadata, ChainStatus,
  HasTimestamp, IndexerClient, InternalCommand, InternalCommandType, IxSearchTxn, MinaMesh, MinaMeshError,
  TransactionStatus, UserCommand, UserCommandMetadata, UserCommandType, ZkAppCommand,
};

impl MinaMesh {
  pub async fn search_transactions(
    &self,
    req: SearchTransactionsRequest,
  ) -> Result<SearchTransactionsResponse, MinaMeshError> {
    self.validate_network(&req.network_identifier).await?;
    // Trustless backend: emulate search over the indexer (no offset/cursor pagination or
    // total_count there). See `search_transactions_indexer` for the degradations.
    if let Some(indexer) = &self.indexer {
      return self.search_transactions_indexer(indexer, &req).await;
    }
    let original_offset = req.offset.unwrap_or(0);
    let mut offset = original_offset;
    let mut limit = req.limit.unwrap_or(100);
    let mut transactions = Vec::new();
    let mut total_count = 0;
    tracing::debug!("{:?}", req);
    tracing::debug!("Offset: {}, Limit: {}", offset, limit);

    let query_params = SearchTransactionsQueryParams::try_from(req.clone())?;
    let include_timestamp = req.include_timestamp.unwrap_or(false);

    // User Commands
    let user_commands = self.fetch_user_commands(&query_params, offset, limit).await?;
    let user_commands_total_count = user_commands.first().and_then(|uc| uc.total_count).unwrap_or(0);
    let user_transactions_bt: Vec<BlockTransaction> = map_to_block_transactions(user_commands, include_timestamp);
    transactions.extend(user_transactions_bt);
    total_count += user_commands_total_count;
    tracing::debug!("User commands total: {}, retrieved: {}", user_commands_total_count, transactions.len());

    // Internal Commands
    let mut internal_commands_bt_len = 0;
    if limit > transactions.len() as i64 {
      // if we are below the limit, fetch internal commands
      (offset, limit) = adjust_limit_and_offset(limit, offset, transactions.len() as i64);
      tracing::debug!("Offset: {}, Limit: {}", offset, limit);
      let internal_commands = self.fetch_internal_commands(&query_params, offset, limit).await?;
      let internal_commands_total_count = internal_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      let internal_commands_bt: Vec<BlockTransaction> = map_to_block_transactions(internal_commands, include_timestamp);
      internal_commands_bt_len = internal_commands_bt.len();
      transactions.extend(internal_commands_bt);
      total_count += internal_commands_total_count;
      tracing::debug!(
        "Internal commands total: {}, retrieved: {}",
        internal_commands_total_count,
        internal_commands_bt_len
      );
    } else {
      // otherwise only fetch the first internal command to get the total count
      let internal_commands = self.fetch_internal_commands(&query_params, 0, 1).await?;
      let internal_commands_total_count = internal_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      total_count += internal_commands_total_count;
      tracing::debug!("Internal commands total: {}", internal_commands_total_count);
    }

    // ZkApp Commands
    if limit > transactions.len() as i64 {
      // if we are below the limit, fetch zkapp commands
      (offset, limit) = adjust_limit_and_offset(limit, offset, internal_commands_bt_len as i64);
      tracing::debug!("Offset: {}, Limit: {}", offset, limit);
      let zkapp_commands = self.fetch_zkapp_commands(&query_params, offset, limit).await?;
      let zkapp_commands_total_count = zkapp_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      let zkapp_commands_bt = zkapp_commands_to_block_transactions(zkapp_commands, include_timestamp);
      let zkapp_commands_bt_len = zkapp_commands_bt.len();
      transactions.extend(zkapp_commands_bt);
      total_count += zkapp_commands_total_count;
      tracing::debug!("Zkapp commands total: {}, retrieved: {}", zkapp_commands_total_count, zkapp_commands_bt_len);
    } else {
      // otherwise only fetch the first zkapp command to get the total count
      let zkapp_commands = self.fetch_zkapp_commands(&query_params, 0, 1).await?;
      let zkapp_commands_total_count = zkapp_commands.first().and_then(|ic| ic.total_count).unwrap_or(0);
      total_count += zkapp_commands_total_count;
      tracing::debug!("Zkapp commands total: {}", zkapp_commands_total_count);
    }

    let next_offset = original_offset + transactions.len() as i64;
    let tx_len = transactions.len() as i64;
    let response = SearchTransactionsResponse {
      transactions,
      total_count,
      next_offset: if next_offset < total_count { Some(next_offset) } else { None },
    };
    tracing::debug!("Total tx count: {}, retrieved: {}, next_offset: {}", total_count, tx_len, next_offset);

    Ok(response)
  }

  /// Search over the trustless indexer. The indexer has no offset/cursor pagination, no
  /// `total_count`, and no combined sender-OR-receiver filter, so we fetch sender and
  /// receiver user commands separately, union + dedupe by hash, filter, and page in Rust.
  /// Degradations vs Postgres: only user commands (no internal/zkApp commands in search);
  /// `total_count` is the size of the fetched window (capped), not the global total; and
  /// per-result timestamps aren't available (the indexer gives ISO, not epoch millis).
  async fn search_transactions_indexer(
    &self,
    indexer: &IndexerClient,
    req: &SearchTransactionsRequest,
  ) -> Result<SearchTransactionsResponse, MinaMeshError> {
    let qp = SearchTransactionsQueryParams::try_from(req.clone())?;
    let include_timestamp = req.include_timestamp.unwrap_or(false);
    let limit = req.limit.unwrap_or(100).max(0) as usize;
    let offset = req.offset.unwrap_or(0).max(0) as usize;
    let max_height = qp.max_block.map(|h| h as u32);

    let mut txns: Vec<IxSearchTxn> = Vec::new();
    if let Some(hash) = &qp.transaction_hash {
      if let Some(t) = indexer.transaction_by_hash(hash).await? {
        txns.push(t);
      }
    } else if let Some(pk) = qp.account_identifier.clone().or_else(|| qp.address.clone()) {
      // Fetch enough rows to cover the requested page; this also bounds total_count.
      let cap = offset + limit.max(1) + 50;
      let outgoing = indexer.account_transactions(&pk, true, max_height, cap).await?;
      let incoming = indexer.account_transactions(&pk, false, max_height, cap).await?;
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
    let transactions: Vec<BlockTransaction> =
      txns.iter().skip(offset).take(limit).map(|t| ix_search_to_block_transaction(t, include_timestamp)).collect();
    let next_offset = offset as i64 + transactions.len() as i64;
    Ok(SearchTransactionsResponse {
      transactions,
      total_count,
      next_offset: if next_offset < total_count { Some(next_offset) } else { None },
    })
  }

  pub async fn fetch_user_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<UserCommand>, MinaMeshError> {
    if !self.search_tx_optimized {
      let user_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;
      Ok(user_commands)
    } else {
      let user_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;
      Ok(user_commands)
    }
  }

  pub async fn fetch_internal_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<InternalCommand>, MinaMeshError> {
    if !self.search_tx_optimized {
      let internal_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;

      Ok(internal_commands)
    } else {
      let internal_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;

      Ok(internal_commands)
    }
  }

  async fn fetch_zkapp_commands(
    &self,
    query_params: &SearchTransactionsQueryParams,
    offset: i64,
    limit: i64,
  ) -> Result<Vec<ZkAppCommand>, MinaMeshError> {
    if !self.search_tx_optimized {
      let zkapp_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;

      Ok(zkapp_commands)
    } else {
      let zkapp_commands = sqlx::query_file_as!(
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
      .fetch_all(self.pg()?)
      .await?;

      Ok(zkapp_commands)
    }
  }
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
      let transaction = BlockTransaction {
        block_identifier: Box::new(BlockIdentifier { index: block_index, hash: block_hash.clone() }),
        transaction: Box::new(Transaction {
          transaction_identifier: Box::new(TransactionIdentifier { hash: tx_hash }),
          operations,
          metadata: None,
          related_transactions: None,
        }),
        timestamp: {
          if include_timestamp {
            let ts = timestamp.clone().unwrap_or_default();
            Some(ts.parse::<i64>().unwrap_or_default())
          } else {
            None
          }
        },
      };
      result.push(transaction);
    }
  }

  result
}

fn map_to_block_transactions<T>(commands: Vec<T>, include_timestamp: bool) -> Vec<BlockTransaction>
where
  T: Into<BlockTransaction> + HasTimestamp,
{
  commands
    .into_iter()
    .map(|cmd| {
      let timestamp = cmd.timestamp().map(|ts| ts.parse::<i64>().unwrap_or_default());
      let mut transaction: BlockTransaction = cmd.into();
      if include_timestamp {
        transaction.timestamp = timestamp;
      } else {
        transaction.timestamp = None;
      }
      transaction
    })
    .collect()
}

impl From<InternalCommand> for BlockTransaction {
  fn from(internal_command: InternalCommand) -> Self {
    // Derive transaction_identifier by combining command_type, sequence numbers,
    // and the hash
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
fn ix_search_to_block_transaction(tx: &IxSearchTxn, include_timestamp: bool) -> BlockTransaction {
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
    creation_fee: tx.receiver_account_creation_fee_paid.then(|| 1_000_000_000u64.to_string()),
  };
  let transaction = Transaction {
    transaction_identifier: Box::new(TransactionIdentifier::new(tx.hash.clone())),
    operations: generate_operations_user_command(&meta),
    metadata: generate_transaction_metadata(&meta),
    related_transactions: None,
  };
  let block_identifier = BlockIdentifier::new(tx.block_height as i64, tx.block.state_hash.clone());
  let mut bt = BlockTransaction::new(block_identifier, transaction);
  // The indexer exposes ISO datetimes, not epoch millis, so per-result timestamps aren't
  // available here (Rosetta wants i64 millis). include_timestamp callers get None.
  if include_timestamp {
    bt.timestamp = tx.block.date_time.parse::<i64>().ok();
  }
  bt
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

impl std::fmt::Display for SearchTransactionsQueryParams {
  fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    write!(
      f,
      "max_block: {:?}, transaction_hash: {:?}, account_identifier: {:?}, token_id: {:?}, status: {:?}, success_status: {:?}, address: {:?}",
      self.max_block, self.transaction_hash, self.account_identifier, self.token_id, self.status, self.success_status, self.address
    )
  }
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

    let address = req.address;
    let st = SearchTransactionsQueryParams {
      max_block,
      transaction_hash,
      account_identifier,
      token_id,
      status,
      success_status,
      address,
    };
    Ok(st)
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
