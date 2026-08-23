//! Archive-Node-API adapter for the [`crate::MinaArchive`] history axis.
//!
//! `Archive-Node-API` (`o1-labs/Archive-Node-API`) is a GraphQL server for zkApp / o1js
//! developers that sits on top of an existing Mina archive-node Postgres and exposes
//! `events`, `actions`, `networkState`, and `blocks`. It is a **partial** history source for
//! Rosetta: its `blocks` query can serve `/block` (by height) and the tip / oldest, but it has
//! **no** ledger-account query, **no** block-by-state-hash filter, and **no** account-scoped
//! transaction search. So this adapter implements the block/tip/oldest surface and returns a
//! clear error for the endpoints the API cannot back (`historical_balance`, `search_transactions`,
//! `account_nonce`). It is the third `MinaArchive` implementation, alongside
//! [`crate::IndexerArchive`] and [`crate::PostgresArchive`].
//!
//! Closing these gaps (so this backend reaches parity with the indexer / raw-SQL paths) is
//! tracked upstream in o1-labs/Archive-Node-API#200.
//!
//! Degradations vs the Postgres archive, all driven by what the API exposes:
//!   * block lookup by **state hash** is unsupported (`BlockQueryInput` has no `stateHash`);
//!   * only **canonical** blocks are served (pending-tip blocks return "not found");
//!   * **coinbase** is not itemized — the API's block carries the coinbase amount but not its
//!     receiver, so it can't be turned into a Rosetta operation (fee transfers, which carry a
//!     recipient, are emitted);
//!   * user commands carry no account-creation-fee flag (`creation_fee: None`);
//!   * zkApp commands are not itemized (same as the indexer path).

use async_trait::async_trait;
use coinbase_mesh::models::{BlockIdentifier, PartialBlockIdentifier, SearchTransactionsRequest};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::json;

use crate::{
  ArchiveAccountBalance, ArchiveBlock, ArchiveTip, ArchiveTransactionPage, InternalCommandMetadata,
  InternalCommandType, MinaArchive, MinaMeshError, Payment, Provenance, TransactionStatus, UserCommandMetadata,
  UserCommandType,
};

/// HTTP GraphQL client for a running `Archive-Node-API` server.
#[derive(Debug, Clone)]
pub struct ArchiveNodeApiClient {
  graphql_url: String,
  http: reqwest::Client,
}

impl ArchiveNodeApiClient {
  pub fn new(base_url: String) -> Self {
    let base = base_url.trim_end_matches('/').to_string();
    // The API serves GraphQL at the root; allow a base that already includes `/graphql`.
    let graphql_url = if base.ends_with("/graphql") { base } else { format!("{base}/graphql") };
    Self { graphql_url, http: reqwest::Client::new() }
  }

  async fn gql<T: DeserializeOwned>(&self, query: String) -> Result<T, MinaMeshError> {
    let resp = self
      .http
      .post(&self.graphql_url)
      .json(&json!({ "query": query }))
      .send()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("archive-node-api GraphQL: {e}")))?;
    if !resp.status().is_success() {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      return Err(MinaMeshError::Exception(format!("archive-node-api GraphQL -> {status}: {body}")));
    }
    let parsed: GqlResponse<T> =
      resp.json().await.map_err(|e| MinaMeshError::Exception(format!("archive-node-api GraphQL decode: {e}")))?;
    if !parsed.errors.is_empty() {
      let msg = parsed.errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join("; ");
      return Err(MinaMeshError::Exception(format!("archive-node-api GraphQL errors: {msg}")));
    }
    parsed.data.ok_or_else(|| MinaMeshError::Exception("archive-node-api GraphQL: empty data".to_string()))
  }
}

// ---------- GraphQL response shapes ----------

#[derive(Debug, Deserialize)]
struct GqlResponse<T> {
  data: Option<T>,
  #[serde(default)]
  errors: Vec<GqlError>,
}

#[derive(Debug, Deserialize)]
struct GqlError {
  message: String,
}

#[derive(Debug, Deserialize)]
struct BlocksData {
  blocks: Vec<AnaBlock>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnaBlock {
  block_height: i64,
  state_hash: String,
  #[serde(default)]
  parent_hash: Option<String>,
  #[serde(default)]
  creator: Option<String>,
  /// ISO-8601 (`toISOString()`), e.g. `2024-01-02T03:04:05.678Z`.
  date_time: String,
  #[serde(default)]
  transactions: Option<AnaTxns>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnaTxns {
  #[serde(default)]
  user_commands: Vec<AnaUserCommand>,
  #[serde(default)]
  fee_transfer: Vec<AnaFeeTransfer>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AnaUserCommand {
  hash: String,
  kind: String,
  from: String,
  to: String,
  amount: String,
  fee: String,
  memo: String,
  nonce: i64,
  status: String,
  failure_reason: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AnaFeeTransfer {
  recipient: String,
  fee: String,
}

impl AnaBlock {
  const HEADER_FIELDS: &'static str = "blockHeight stateHash parentHash creator dateTime";
  const FULL_FIELDS: &'static str = "blockHeight stateHash parentHash creator dateTime \
    transactions { coinbase \
      userCommands { hash kind from to amount fee memo nonce status failureReason } \
      feeTransfer { recipient fee type } }";
}

/// Archive-Node-API history adapter. `provenance() == TrustedArchive` — it reads the same
/// archive Postgres a [`crate::PostgresArchive`] would, just through the API's GraphQL.
#[derive(Debug)]
pub struct ArchiveNodeApiArchive {
  client: ArchiveNodeApiClient,
}

impl ArchiveNodeApiArchive {
  pub fn new(client: ArchiveNodeApiClient) -> Self {
    Self { client }
  }

  /// Fetch a single canonical block by height (with `fields`), newest match first.
  async fn canonical_block_at(&self, height: i64, fields: &str) -> Result<Option<AnaBlock>, MinaMeshError> {
    let q = format!(
      "query {{ blocks(query: {{ blockHeight_gte: {height}, blockHeight_lt: {}, canonical: true }}, \
        limit: 1, sortBy: BLOCKHEIGHT_DESC) {{ {fields} }} }}",
      height + 1
    );
    Ok(self.client.gql::<BlocksData>(q).await?.blocks.into_iter().next())
  }

  /// Fetch the extreme canonical block (`BLOCKHEIGHT_DESC` = tip, `_ASC` = oldest).
  async fn extreme_block(&self, desc: bool, fields: &str) -> Result<AnaBlock, MinaMeshError> {
    let sort = if desc { "BLOCKHEIGHT_DESC" } else { "BLOCKHEIGHT_ASC" };
    let q = format!("query {{ blocks(query: {{ canonical: true }}, limit: 1, sortBy: {sort}) {{ {fields} }} }}");
    self.client.gql::<BlocksData>(q).await?.blocks.into_iter().next().ok_or(MinaMeshError::ChainInfoMissing)
  }

  /// Assemble a Rosetta block from an Archive-Node-API block. See the module docs for the
  /// coinbase / creation-fee / zkApp degradations.
  fn to_archive_block(block: AnaBlock) -> Result<ArchiveBlock, MinaMeshError> {
    let block_identifier = BlockIdentifier::new(block.block_height, block.state_hash.clone());
    // Blocks are height-contiguous; the parent is height-1 with the reported parent hash.
    // Genesis links to itself.
    let parent_block_identifier = match (&block.parent_hash, block.block_height <= 1) {
      (Some(parent_hash), false) => BlockIdentifier::new(block.block_height - 1, parent_hash.clone()),
      _ => block_identifier.clone(),
    };
    let timestamp = iso8601_to_millis(&block.date_time)
      .ok_or_else(|| MinaMeshError::Exception(format!("archive-node-api: unparseable dateTime {}", block.date_time)))?;

    let mut user_commands: Vec<UserCommandMetadata> = Vec::new();
    let mut internal_commands: Vec<InternalCommandMetadata> = Vec::new();
    let txns = block.transactions.unwrap_or(AnaTxns { user_commands: vec![], fee_transfer: vec![] });

    // User commands (payments / delegations). No account-creation-fee flag is exposed, so
    // `creation_fee` is always `None` here.
    for uc in txns.user_commands {
      let command_type =
        if uc.kind.to_uppercase().contains("DELEG") { UserCommandType::Delegation } else { UserCommandType::Payment };
      let amount = match command_type {
        UserCommandType::Payment => Some(uc.amount),
        UserCommandType::Delegation => None,
      };
      let meta = UserCommandMetadata {
        command_type,
        nonce: uc.nonce,
        amount,
        fee: Some(uc.fee),
        valid_until: None,
        memo: Some(uc.memo),
        hash: uc.hash.clone(),
        fee_payer: uc.from.clone(),
        source: uc.from,
        receiver: uc.to,
        status: if uc.status.eq_ignore_ascii_case("applied") {
          TransactionStatus::Applied
        } else {
          TransactionStatus::Failed
        },
        failure_reason: uc.failure_reason,
        creation_fee: None,
      };
      user_commands.push(meta);
    }

    // Fee transfers (all treated as plain, applied). Coinbase is intentionally skipped — the
    // API does not expose its receiver, so it can't be turned into a Rosetta operation.
    for (seq, ft) in txns.fee_transfer.into_iter().enumerate() {
      let fee = ft.fee.parse::<u64>().unwrap_or(0);
      if fee == 0 {
        continue;
      }
      let meta = InternalCommandMetadata {
        command_type: InternalCommandType::FeeTransfer,
        receiver: ft.recipient.clone(),
        fee: Some(fee.to_string()),
        hash: block.state_hash.clone(),
        creation_fee: None,
        sequence_no: seq as i32,
        secondary_sequence_no: 0,
        status: TransactionStatus::Applied,
        coinbase_receiver: block.creator.clone(),
      };
      internal_commands.push(meta);
    }

    Ok(ArchiveBlock {
      block_identifier,
      parent_block_identifier,
      timestamp,
      creator: block.creator,
      user_commands,
      internal_commands,
      // The archive-node-api surface does not expose zkApp commands.
      zkapp_commands: Vec::new(),
    })
  }
}

#[async_trait]
impl MinaArchive for ArchiveNodeApiArchive {
  fn provenance(&self) -> Provenance {
    Provenance::TrustedArchive
  }

  async fn tip(&self) -> Result<ArchiveTip, MinaMeshError> {
    let b = self.extreme_block(true, AnaBlock::HEADER_FIELDS).await?;
    let timestamp = iso8601_to_millis(&b.date_time)
      .ok_or_else(|| MinaMeshError::Exception(format!("archive-node-api: unparseable dateTime {}", b.date_time)))?;
    Ok(ArchiveTip { block_identifier: BlockIdentifier::new(b.block_height, b.state_hash), timestamp })
  }

  async fn oldest_block_identifier(&self) -> Result<BlockIdentifier, MinaMeshError> {
    let b = self.extreme_block(false, AnaBlock::HEADER_FIELDS).await?;
    Ok(BlockIdentifier::new(b.block_height, b.state_hash))
  }

  async fn block(&self, partial: &PartialBlockIdentifier) -> Result<ArchiveBlock, MinaMeshError> {
    let block = match (&partial.hash, partial.index) {
      // The API's `BlockQueryInput` has no `stateHash` filter, so hash lookups aren't possible.
      // Tracked upstream: o1-labs/Archive-Node-API#200.
      (Some(_), _) => {
        return Err(MinaMeshError::Exception(
          "archive-node-api backend cannot look up a block by state hash (query by index instead)".to_string(),
        ));
      }
      (None, Some(idx)) => self.canonical_block_at(idx, AnaBlock::FULL_FIELDS).await?,
      (None, None) => Some(self.extreme_block(true, AnaBlock::FULL_FIELDS).await?),
    }
    .ok_or_else(|| MinaMeshError::BlockMissing(partial.index, partial.hash.clone()))?;
    Self::to_archive_block(block)
  }

  async fn historical_balance(
    &self,
    _public_key: &str,
    _metadata: Option<serde_json::Value>,
    _partial: &PartialBlockIdentifier,
  ) -> Result<ArchiveAccountBalance, MinaMeshError> {
    // Needs a historical ledger-account query — tracked upstream: o1-labs/Archive-Node-API#200.
    Err(MinaMeshError::Exception(
      "archive-node-api backend does not expose ledger account state; historical balance is unsupported".to_string(),
    ))
  }

  async fn search_transactions(
    &self,
    _req: &SearchTransactionsRequest,
  ) -> Result<ArchiveTransactionPage, MinaMeshError> {
    // Needs an account-scoped transaction query — tracked upstream: o1-labs/Archive-Node-API#200.
    Err(MinaMeshError::Exception(
      "archive-node-api backend does not support account-scoped transaction search".to_string(),
    ))
  }

  async fn account_nonce(&self, _public_key: &str) -> Result<Option<u32>, MinaMeshError> {
    // Needs an account/nonce query — tracked upstream: o1-labs/Archive-Node-API#200.
    Err(MinaMeshError::Exception(
      "archive-node-api backend does not expose account nonce; use the daemon (full mode)".to_string(),
    ))
  }

  async fn payment_in_history(&self, _payment: &Payment) -> Result<bool, MinaMeshError> {
    // No tx-by-hash / account search on this backend, so an exact-duplicate check isn't
    // possible. Report "not found": a genuine duplicate then surfaces as a bad-nonce submit
    // error rather than the more specific duplicate error (this path is best-effort refinement).
    Ok(false)
  }
}

/// Parse the fixed `Date.prototype.toISOString()` shape (`YYYY-MM-DDTHH:MM:SS.sssZ`, always
/// UTC) into unix milliseconds, without pulling in a date library. Uses Howard Hinnant's
/// days-from-civil algorithm. Returns `None` on any malformed field.
fn iso8601_to_millis(s: &str) -> Option<i64> {
  let (date, rest) = s.split_once('T')?;
  let mut dp = date.split('-');
  let y: i64 = dp.next()?.parse().ok()?;
  let mo: i64 = dp.next()?.parse().ok()?;
  let d: i64 = dp.next()?.parse().ok()?;

  let time = rest.trim_end_matches('Z');
  let (hms, frac) = time.split_once('.').unwrap_or((time, ""));
  let mut tp = hms.split(':');
  let h: i64 = tp.next()?.parse().ok()?;
  let mi: i64 = tp.next()?.parse().ok()?;
  let se: i64 = tp.next().unwrap_or("0").parse().ok()?;
  // Left-pad/truncate the fractional part to exactly 3 digits (milliseconds).
  let millis: i64 = if frac.is_empty() {
    0
  } else {
    let mut f = frac.chars().filter(|c| c.is_ascii_digit()).collect::<String>();
    f.truncate(3);
    while f.len() < 3 {
      f.push('0');
    }
    f.parse().ok()?
  };

  // days_from_civil (proleptic Gregorian), epoch = 1970-01-01.
  let yy = if mo <= 2 { y - 1 } else { y };
  let era = (if yy >= 0 { yy } else { yy - 399 }) / 400;
  let yoe = yy - era * 400;
  let doy = (153 * (if mo > 2 { mo - 3 } else { mo + 9 }) + 2) / 5 + d - 1;
  let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  let days = era * 146097 + doe - 719468;
  Some((days * 86400 + h * 3600 + mi * 60 + se) * 1000 + millis)
}

#[cfg(test)]
mod tests {
  use super::iso8601_to_millis;

  #[test]
  fn parses_iso_utc_millis() {
    // 2024-01-02T03:04:05.678Z == 1704164645678 ms since epoch.
    assert_eq!(iso8601_to_millis("2024-01-02T03:04:05.678Z"), Some(1_704_164_645_678));
    // No fractional seconds.
    assert_eq!(iso8601_to_millis("1970-01-01T00:00:00Z"), Some(0));
    assert_eq!(iso8601_to_millis("1970-01-01T00:00:01.000Z"), Some(1000));
    assert_eq!(iso8601_to_millis("not-a-date"), None);
  }
}
