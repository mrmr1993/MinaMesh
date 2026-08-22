//! Client for the trustless **mina-indexer** GraphQL surface.
//!
//! When configured (`MINAMESH_INDEXER_URL`), MinaMesh serves the HISTORICAL/archive
//! reads — block, historical balance, transaction search, the network oldest block —
//! from the mina-indexer instead of a Postgres archive. The indexer ingests a block
//! only after its Pickles/kimchi SNARK proof verifies, so these reads are trustless
//! (it trusts math, not whoever served the block). Pairs with [`crate::LightNodeClient`],
//! which serves the live state (frontier balance, mempool, submit).
//!
//! Everything goes over the indexer's GraphQL endpoint (`<base>/graphql`); we select
//! only the fields each Rosetta endpoint needs and deserialize them into the small
//! structs below. Field names are the indexer's wire names (async-graphql camelCases
//! snake_case fields unless a `#[graphql(name=…)]` overrides it).

use serde::{de::DeserializeOwned, Deserialize};

use crate::MinaMeshError;

/// HTTP client for a running `mina-indexer` web server (REST + GraphQL on :8080).
#[derive(Debug, Clone)]
pub struct IndexerClient {
  graphql_url: String,
  http: reqwest::Client,
}

// ---------- GraphQL response envelope ----------

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

// ---------- typed response shapes ----------

/// A public-key wrapper (the indexer's `PK` object).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxPk {
  pub public_key: String,
}

/// The chain tip + genesis hash, from the best canonical block.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxTip {
  pub state_hash: String,
  pub block_height: u32,
  pub genesis_state_hash: String,
  pub protocol_state: IxTipProtocolState,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxTipProtocolState {
  pub blockchain_state: IxTipBlockchainState,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxTipBlockchainState {
  /// Block timestamp as a numeric (millis) string.
  pub utc_date: String,
}

/// A full block with its inline commands — covers a Rosetta `/block` in one query.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxBlock {
  pub state_hash: String,
  pub block_height: u32,
  pub global_slot_since_genesis: u32,
  pub canonical: bool,
  pub creator_account: IxPk,
  pub protocol_state: IxProtocolState,
  pub transactions: IxBlockTxns,
  /// SNARK-work fees the block producer pays out of the fee pool (nanomina). Needed so the
  /// producer's fee credit is reported net of them (and non-producer provers are credited).
  #[serde(default)]
  pub snark_jobs: Vec<IxSnarkJob>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxSnarkJob {
  pub fee: u64,
  pub prover: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxProtocolState {
  pub previous_state_hash: String,
  pub blockchain_state: IxBlockchainState,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxBlockchainState {
  /// Block timestamp as a numeric (millis) string.
  pub utc_date: String,
}

/// The commands carried by a block: coinbase + fee transfers (internal) and user commands.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxBlockTxns {
  /// Coinbase amount (nanomina, as a string; "0" when none).
  pub coinbase: String,
  pub coinbase_receiver: Option<String>,
  /// Whether the coinbase credited a brand-new account (the receiver paid a creation fee).
  #[serde(rename = "coinbase_receiver_account_creation_fee_paid", default)]
  pub coinbase_receiver_account_creation_fee_paid: bool,
  #[serde(default)]
  pub fee_transfer: Vec<IxFeeTransfer>,
  #[serde(default)]
  pub user_commands: Vec<IxUserCommand>,
  /// zkApp commands. The indexer serves these separately from `userCommands`, mirroring the
  /// archive node's layout.
  #[serde(default)]
  pub zkapp_commands: Vec<IxZkAppCommand>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxZkAppCommand {
  pub hash: String,
  pub fee_payer: String,
  /// Fee in nanomina, as a string.
  pub fee: String,
  pub memo: String,
  /// "applied" | "failed".
  pub status: String,
  pub failure_reason: Option<String>,
  /// Per-account balance changes, in the order the ledger applies them, with updates nested
  /// under `calls` flattened in. Without these the command's effect on balances is invisible.
  #[serde(default)]
  pub account_updates: Vec<IxZkAppAccountUpdate>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxZkAppAccountUpdate {
  pub public_key: String,
  pub token: String,
  /// Signed balance change in nanomina, as a string. Negative for a debit.
  pub balance_change: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxFeeTransfer {
  /// Fee (MINA, as a string) — the indexer renders fee transfers in whole MINA.
  pub fee: String,
  pub recipient: String,
  #[serde(rename = "type")]
  pub kind: String,
}

/// A user command (payment / delegation), as served inside a block or by a search.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxUserCommand {
  pub amount: u64,
  pub fee: u64,
  pub from: String,
  pub to: Option<String>,
  pub nonce: u32,
  pub memo: String,
  pub hash: String,
  /// `PAYMENT` | `STAKE_DELEGATION` (indexer kinds).
  pub kind: String,
  pub failure_reason: Option<String>,
  pub is_applied: bool,
  /// Whether this command paid the receiver's account-creation fee (new account).
  #[serde(rename = "receiver_account_creation_fee_paid", default)]
  pub receiver_account_creation_fee_paid: bool,
}

/// A user command with its containing-block context — the shape `search/transactions`
/// needs (each becomes a Rosetta `BlockTransaction`).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxSearchTxn {
  pub amount: u64,
  pub fee: u64,
  pub from: String,
  pub to: Option<String>,
  pub nonce: u32,
  pub memo: String,
  pub hash: String,
  pub kind: String,
  pub failure_reason: Option<String>,
  pub is_applied: bool,
  pub canonical: bool,
  pub block_height: u32,
  pub block: IxTxnBlock,
  #[serde(rename = "receiver_account_creation_fee_paid", default)]
  pub receiver_account_creation_fee_paid: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxTxnBlock {
  pub state_hash: String,
  pub date_time: String,
}

/// A staged-ledger account snapshot (balance/nonce at a height or state hash).
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IxStagedAccount {
  // The indexer pins this field's wire name to snake_case (`#[graphql(name = "balance_nano")]`),
  // unlike its other camelCased fields — so query + deserialize it as `balance_nano`.
  #[serde(rename = "balance_nano")]
  pub balance_nano: u64,
  pub nonce: u32,
  pub token: String,
}

impl IndexerClient {
  pub fn new(base_url: String) -> Self {
    let base = base_url.trim_end_matches('/').to_string();
    Self { graphql_url: format!("{base}/graphql"), http: reqwest::Client::new() }
  }

  /// POST a GraphQL query and deserialize `data` into `T`.
  async fn gql<T: DeserializeOwned>(&self, query: String) -> Result<T, MinaMeshError> {
    let resp = self
      .http
      .post(&self.graphql_url)
      .json(&serde_json::json!({ "query": query }))
      .send()
      .await
      .map_err(|e| MinaMeshError::Exception(format!("indexer GraphQL: {e}")))?;
    if !resp.status().is_success() {
      let status = resp.status();
      let body = resp.text().await.unwrap_or_default();
      return Err(MinaMeshError::Exception(format!("indexer GraphQL -> {status}: {body}")));
    }
    let parsed: GqlResponse<T> =
      resp.json().await.map_err(|e| MinaMeshError::Exception(format!("indexer GraphQL decode: {e}")))?;
    if !parsed.errors.is_empty() {
      let msg = parsed.errors.into_iter().map(|e| e.message).collect::<Vec<_>>().join("; ");
      return Err(MinaMeshError::Exception(format!("indexer GraphQL errors: {msg}")));
    }
    parsed.data.ok_or_else(|| MinaMeshError::Exception("indexer GraphQL: empty data".to_string()))
  }

  /// JSON-encode a value for safe inlining into a GraphQL query literal.
  fn lit(s: &str) -> String {
    serde_json::Value::String(s.to_string()).to_string()
  }

  /// The best canonical block — chain tip (+ genesis hash + timestamp).
  pub async fn tip(&self) -> Result<IxTip, MinaMeshError> {
    #[derive(Deserialize)]
    struct R {
      blocks: Vec<IxTip>,
    }
    let q = r#"query { blocks(query: { canonical: true }, limit: 1, sortBy: BLOCKHEIGHT_DESC) {
      stateHash blockHeight genesisStateHash
      protocolState { blockchainState { utcDate } }
    } }"#
      .to_string();
    let r: R = self.gql(q).await?;
    r.blocks.into_iter().next().ok_or(MinaMeshError::ChainInfoMissing)
  }

  /// The earliest canonical block the indexer holds — the Rosetta `oldest_block` (archive
  /// availability floor). For a from-genesis indexer this is genesis.
  pub async fn oldest(&self) -> Result<IxTip, MinaMeshError> {
    #[derive(Deserialize)]
    struct R {
      blocks: Vec<IxTip>,
    }
    let q = r#"query { blocks(query: { canonical: true }, limit: 1, sortBy: BLOCKHEIGHT_ASC) {
      stateHash blockHeight genesisStateHash
      protocolState { blockchainState { utcDate } }
    } }"#
      .to_string();
    let r: R = self.gql(q).await?;
    r.blocks.into_iter().next().ok_or(MinaMeshError::ChainInfoMissing)
  }

  /// The canonical block at `height`, or the block with `state_hash`, with its commands.
  /// At least one of the two must be `Some`.
  pub async fn block(&self, height: Option<i64>, state_hash: Option<&str>) -> Result<Option<IxBlock>, MinaMeshError> {
    let filter = match (height, state_hash) {
      (_, Some(h)) => format!("stateHash: {}", Self::lit(h)),
      (Some(idx), None) => format!("blockHeight: {idx}, canonical: true"),
      (None, None) => return Err(MinaMeshError::InvariantViolation),
    };
    #[derive(Deserialize)]
    struct R {
      block: Option<IxBlock>,
    }
    let q = format!(
      r#"query {{ block(query: {{ {filter} }}) {{
        stateHash blockHeight globalSlotSinceGenesis canonical
        creatorAccount {{ publicKey }}
        protocolState {{ previousStateHash blockchainState {{ utcDate }} }}
        transactions {{
          coinbase coinbaseReceiver coinbase_receiver_account_creation_fee_paid
          feeTransfer {{ fee recipient type }}
          userCommands {{ amount fee from to nonce memo hash kind failureReason isApplied receiver_account_creation_fee_paid }}
          zkappCommands {{
            hash feePayer fee memo status failureReason
            accountUpdates {{ publicKey token balanceChange }}
          }}
        }}
        snarkJobs {{ fee prover }}
      }} }}"#
    );
    let r: R = self.gql(q).await?;
    Ok(r.block)
  }

  /// Historical balance/nonce for `public_key` as of canonical block `height`, from the
  /// staged ledger. `None` if the account isn't present at that height.
  pub async fn staged_account(
    &self,
    public_key: &str,
    height: u32,
    token: Option<&str>,
  ) -> Result<Option<IxStagedAccount>, MinaMeshError> {
    let token_filter = token.map(|t| format!(", token: {}", Self::lit(t))).unwrap_or_default();
    #[derive(Deserialize)]
    struct R {
      #[serde(rename = "stagedLedgerAccounts")]
      staged_ledger_accounts: Vec<IxStagedAccount>,
    }
    let q = format!(
      r#"query {{ stagedLedgerAccounts(query: {{ publicKey: {}, blockchain_length: {height}{token_filter} }}) {{
        balance_nano nonce token
      }} }}"#,
      Self::lit(public_key)
    );
    let r: R = self.gql(q).await?;
    Ok(r.staged_ledger_accounts.into_iter().next())
  }

  /// User commands touching `public_key` as sender (`outgoing=true`) or receiver
  /// (`outgoing=false`), at or below `max_height`, newest first, capped at `limit`.
  /// `search/transactions` unions sender + receiver and dedupes by hash client-side
  /// (the indexer has no combined from-OR-to filter and no offset pagination).
  pub async fn account_transactions(
    &self,
    public_key: &str,
    outgoing: bool,
    max_height: Option<u32>,
    limit: usize,
  ) -> Result<Vec<IxSearchTxn>, MinaMeshError> {
    let dir = if outgoing { "from" } else { "to" };
    let height_filter = max_height.map(|h| format!(", blockHeight_lte: {h}")).unwrap_or_default();
    #[derive(Deserialize)]
    struct R {
      transactions: Vec<IxSearchTxn>,
    }
    let q = format!(
      r#"query {{ transactions(query: {{ {dir}: {}{height_filter} }}, limit: {limit}, sortBy: BLOCKHEIGHT_DESC) {{
        amount fee from to nonce memo hash kind failureReason isApplied canonical blockHeight
        receiver_account_creation_fee_paid block {{ stateHash dateTime }}
      }} }}"#,
      Self::lit(public_key)
    );
    let r: R = self.gql(q).await?;
    Ok(r.transactions)
  }

  /// The best (latest) account nonce, or `None` if the account doesn't exist yet — used by
  /// `construction/metadata` (current nonce to build a tx; receiver existence ⇒ creation fee).
  pub async fn account_nonce(&self, public_key: &str) -> Result<Option<u32>, MinaMeshError> {
    #[derive(Deserialize)]
    struct A {
      nonce: u32,
    }
    #[derive(Deserialize)]
    struct R {
      accounts: Vec<A>,
    }
    let q = format!(r#"query {{ accounts(query: {{ publicKey: {} }}, limit: 1) {{ nonce }} }}"#, Self::lit(public_key));
    let r: R = self.gql(q).await?;
    Ok(r.accounts.into_iter().next().map(|a| a.nonce))
  }

  /// A single user command by hash (duplicate detection for `construction/submit`).
  pub async fn transaction_by_hash(&self, hash: &str) -> Result<Option<IxSearchTxn>, MinaMeshError> {
    #[derive(Deserialize)]
    struct R {
      transactions: Vec<IxSearchTxn>,
    }
    let q = format!(
      r#"query {{ transactions(query: {{ hash: {} }}, limit: 1) {{
        amount fee from to nonce memo hash kind failureReason isApplied canonical blockHeight
        receiver_account_creation_fee_paid block {{ stateHash dateTime }}
      }} }}"#,
      Self::lit(hash)
    );
    let r: R = self.gql(q).await?;
    Ok(r.transactions.into_iter().next())
  }
}
