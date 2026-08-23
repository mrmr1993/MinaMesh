//! Hermetic integration test for the `ArchiveNodeApiArchive` history adapter.
//!
//! Spins up a tiny local `axum` server that mimics the `Archive-Node-API` GraphQL surface
//! (the `blocks` query) with canned responses, then drives the real adapter against it. No
//! external services, DB, or network — fully deterministic.
//!
//! Covers both halves of the adapter's contract:
//!   * what it *can* serve — `tip` / `oldest` / `block`-by-index, including ISO→millis
//!     timestamps, parent linkage, and that coinbase is skipped while fee transfers +
//!     user commands are emitted;
//!   * what it *cannot* — `block`-by-hash, historical balance, search, and account nonce all
//!     return a clear error (tracked upstream in o1-labs/Archive-Node-API#200).

use anyhow::Result;
use axum::{routing::post, Json, Router};
use coinbase_mesh::models::PartialBlockIdentifier;
use mina_mesh::{
  models::{BlockResponse, SearchTransactionsRequest},
  ArchiveNodeApiArchive, ArchiveNodeApiClient, MinaArchive, Payment, PaymentHistory, Provenance,
};
use pretty_assertions::assert_eq;
use serde_json::{json, Value};

/// The tip / best canonical block (height 100) with one payment, one delegation, one fee
/// transfer, and a coinbase amount (which the adapter must skip — no receiver is exposed).
const TIP_HEIGHT: i64 = 100;
const TIP_HASH: &str = "3NBlock100Hash";
const PARENT_HASH: &str = "3NBlock099Hash";
const PAYMENT_HASH: &str = "CkPaYmentHash";
const DELEGATION_HASH: &str = "CkDelegationHash";
const CREATOR: &str = "B62qCreatorProducer";
/// 2024-01-02T03:04:05.678Z in unix millis.
const TIP_TS_MILLIS: i64 = 1_704_164_645_678;

fn full_block_json() -> Value {
  json!({
    "blockHeight": TIP_HEIGHT,
    "stateHash": TIP_HASH,
    "parentHash": PARENT_HASH,
    "creator": CREATOR,
    "dateTime": "2024-01-02T03:04:05.678Z",
    "transactions": {
      "coinbase": "720000000000",
      "userCommands": [
        {
          "hash": PAYMENT_HASH, "kind": "PAYMENT",
          "from": "B62qSender", "to": "B62qReceiver",
          "amount": "1000000000", "fee": "10000000",
          "memo": "", "nonce": 7, "status": "applied", "failureReason": null
        },
        {
          "hash": DELEGATION_HASH, "kind": "STAKE_DELEGATION",
          "from": "B62qDelegator", "to": "B62qDelegate",
          "amount": "0", "fee": "10000000",
          "memo": "", "nonce": 8, "status": "applied", "failureReason": null
        }
      ],
      "feeTransfer": [
        { "recipient": "B62qProver", "fee": "5000000", "type": "Fee_transfer" }
      ]
    }
  })
}

/// The oldest canonical block (height 1) — genesis for a from-genesis archive.
fn oldest_block_json() -> Value {
  json!({
    "blockHeight": 1,
    "stateHash": "3NGenesisHash",
    "parentHash": "3NGenesisHash",
    "creator": CREATOR,
    "dateTime": "2024-01-01T00:00:00.000Z"
  })
}

/// Mock GraphQL handler: routes by the shape of the query string the adapter sends.
async fn graphql(Json(body): Json<Value>) -> Json<Value> {
  let query = body.get("query").and_then(|q| q.as_str()).unwrap_or_default();
  let block = if query.contains("BLOCKHEIGHT_ASC") {
    // oldest_block_identifier()
    oldest_block_json()
  } else {
    // tip() / best / block-at-height (all BLOCKHEIGHT_DESC); the full block satisfies both the
    // header-only and full field selections.
    full_block_json()
  };
  Json(json!({ "data": { "blocks": [block] } }))
}

/// Start the mock server on an ephemeral port and return a client pointed at it.
async fn start_mock() -> Result<ArchiveNodeApiClient> {
  let app = Router::new().route("/graphql", post(graphql));
  let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
  let addr = listener.local_addr()?;
  tokio::spawn(async move {
    axum::serve(listener, app).await.unwrap();
  });
  Ok(ArchiveNodeApiClient::new(format!("http://{addr}")))
}

fn archive(client: ArchiveNodeApiClient) -> ArchiveNodeApiArchive {
  ArchiveNodeApiArchive::new(client)
}

#[tokio::test]
async fn provenance_is_trusted_archive() -> Result<()> {
  let a = archive(start_mock().await?);
  assert_eq!(a.provenance(), Provenance::TrustedArchive);
  Ok(())
}

#[tokio::test]
async fn tip_parses_height_hash_and_iso_timestamp() -> Result<()> {
  let a = archive(start_mock().await?);
  let tip = a.tip().await?;
  assert_eq!(tip.block_identifier.index, TIP_HEIGHT);
  assert_eq!(tip.block_identifier.hash, TIP_HASH);
  // ISO-8601 dateTime converted to unix millis without a date library.
  assert_eq!(tip.timestamp, TIP_TS_MILLIS);
  Ok(())
}

#[tokio::test]
async fn oldest_block_identifier_is_genesis() -> Result<()> {
  let a = archive(start_mock().await?);
  let oldest = a.oldest_block_identifier().await?;
  assert_eq!(oldest.index, 1);
  assert_eq!(oldest.hash, "3NGenesisHash");
  Ok(())
}

#[tokio::test]
async fn block_by_index_emits_user_commands_and_fee_transfer_but_not_coinbase() -> Result<()> {
  let a = archive(start_mock().await?);
  // The adapter returns the block and its commands; Rosetta assembly is shared, so exercise it
  // here too rather than asserting on a shape no caller sees.
  let resp: BlockResponse = a.block(&PartialBlockIdentifier { index: Some(TIP_HEIGHT), hash: None }).await?.into();
  let block = resp.block.expect("block present");

  assert_eq!(block.block_identifier.index, TIP_HEIGHT);
  assert_eq!(block.block_identifier.hash, TIP_HASH);
  // Parent is height-1 with the reported parent hash.
  assert_eq!(block.parent_block_identifier.index, TIP_HEIGHT - 1);
  assert_eq!(block.parent_block_identifier.hash, PARENT_HASH);
  assert_eq!(block.timestamp, TIP_TS_MILLIS);

  // Exactly three transactions: payment + delegation + one fee transfer. Coinbase is NOT
  // itemized (the API doesn't expose the coinbase receiver), so no coinbase op is emitted.
  assert_eq!(block.transactions.len(), 3, "coinbase must be skipped; only 2 user cmds + 1 fee transfer");
  let hashes: Vec<&str> = block.transactions.iter().map(|t| t.transaction_identifier.hash.as_str()).collect();
  assert!(hashes.contains(&PAYMENT_HASH), "payment tx present");
  assert!(hashes.contains(&DELEGATION_HASH), "delegation tx present");

  // The payment carries a real transfer amount in its operations; the delegation does not.
  let payment = block.transactions.iter().find(|t| t.transaction_identifier.hash == PAYMENT_HASH).unwrap();
  assert!(
    payment.operations.iter().any(|op| op.amount.as_ref().is_some_and(|amt| amt.value == "1000000000")),
    "payment operation reflects the 1 MINA transfer amount"
  );
  Ok(())
}

#[tokio::test]
async fn block_by_state_hash_is_unsupported() -> Result<()> {
  let a = archive(start_mock().await?);
  let err = a
    .block(&PartialBlockIdentifier { index: None, hash: Some(TIP_HASH.to_string()) })
    .await
    .expect_err("block-by-hash must be rejected");
  assert!(err.to_string().to_lowercase().contains("state hash"), "got: {err}");
  Ok(())
}

#[tokio::test]
async fn historical_balance_is_unsupported() -> Result<()> {
  let a = archive(start_mock().await?);
  let err = a
    .historical_balance("B62qSender", None, &PartialBlockIdentifier { index: Some(TIP_HEIGHT), hash: None })
    .await
    .expect_err("historical balance must be rejected");
  assert!(err.to_string().to_lowercase().contains("ledger account state"), "got: {err}");
  Ok(())
}

#[tokio::test]
async fn search_transactions_is_unsupported() -> Result<()> {
  let a = archive(start_mock().await?);
  let err = a
    .search_transactions(&SearchTransactionsRequest::new(mina_mesh::test::network_id()))
    .await
    .expect_err("search must be rejected");
  assert!(err.to_string().to_lowercase().contains("search"), "got: {err}");
  Ok(())
}

#[tokio::test]
async fn account_nonce_is_unsupported() -> Result<()> {
  let a = archive(start_mock().await?);
  let err = a.account_nonce("B62qSender").await.expect_err("account nonce must be rejected");
  assert!(err.to_string().to_lowercase().contains("nonce"), "got: {err}");
  Ok(())
}

#[tokio::test]
async fn payment_in_history_reports_not_found() -> Result<()> {
  let a = archive(start_mock().await?);
  // No tx-by-hash query on this backend, so duplicate detection is best-effort: reports false.
  let payment = Payment {
    to: "B62qReceiver".to_string(),
    from: "B62qSender".to_string(),
    token: mina_mesh::util::DEFAULT_TOKEN_ID.to_string(),
    amount: 1_000_000_000,
    fee: 10_000_000,
    nonce: 7,
    valid_until: None,
    memo: None,
  };
  // This backend cannot search history, so it reports that rather than a false negative.
  assert_eq!(a.payment_in_history(&payment).await?, PaymentHistory::Unknown);
  Ok(())
}
