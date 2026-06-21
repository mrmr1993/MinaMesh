mod api;
mod commands;
mod config;
mod create_router;
mod error;
mod graphql;
mod indexer;
mod light_node;
pub mod memo;
mod playground;
mod roinput;
pub mod signer_utils;
pub mod test;
mod transaction_operations;
mod types;
pub mod util;

use std::time::{Duration, Instant};

pub use coinbase_mesh::models;
use coinbase_mesh::models::BlockIdentifier;
pub use commands::*;
pub use config::*;
pub use create_router::create_router;
use dashmap::DashMap;
pub use error::*;
use graphql::GraphQLClient;
pub use indexer::*;
pub use light_node::*;
pub(crate) use roinput::*;
use sqlx::PgPool;
pub use transaction_operations::*;
pub use types::*;
#[derive(Debug)]
pub struct MinaMesh {
  pub graphql_client: GraphQLClient,
  /// The Rosetta network id this server serves, `mina:<network>`. In trustless mode it's the
  /// source of truth for network validation / `/network/list` (no daemon query needed).
  pub network_id: String,
  /// Archive Postgres. `None` when the trustless [`IndexerClient`] backs historical reads.
  pub pg_pool: Option<PgPool>,
  pub genesis_block_identifier: BlockIdentifier,
  pub search_tx_optimized: bool,
  pub cache: DashMap<String, (String, Instant)>, // Cache for network_id or other reusable data
  pub cache_ttl: Duration,                       /* Cache time-to-live (network_id is refreshed after this time) */
  pub cache_tx_size: usize,                      // Cache limit for last n transactions submitted
  /// Optional trustless backend: when set, live-state endpoints (mempool, frontier
  /// balance, submit) are served from the mina-light-node instead of the GraphQL
  /// daemon. See [`LightNodeClient`].
  pub light_node: Option<LightNodeClient>,
  /// Optional trustless backend: when set, historical reads (block, historical balance,
  /// search, oldest block) are served from the mina-indexer instead of Postgres. See
  /// [`IndexerClient`].
  pub indexer: Option<IndexerClient>,
}

impl MinaMesh {
  /// The archive Postgres pool, or an error when only the trustless indexer is configured.
  /// Used by the PG fallback path of historical endpoints.
  pub(crate) fn pg(&self) -> Result<&PgPool, MinaMeshError> {
    self
      .pg_pool
      .as_ref()
      .ok_or_else(|| MinaMeshError::Exception("no archive database configured (indexer-only mode)".to_string()))
  }
}
