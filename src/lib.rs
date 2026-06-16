mod api;
mod commands;
mod config;
mod create_router;
mod error;
mod graphql;
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
pub use light_node::*;
pub(crate) use roinput::*;
use sqlx::PgPool;
pub use transaction_operations::*;
pub use types::*;
#[derive(Debug)]
pub struct MinaMesh {
  pub graphql_client: GraphQLClient,
  pub pg_pool: PgPool,
  pub genesis_block_identifier: BlockIdentifier,
  pub search_tx_optimized: bool,
  pub cache: DashMap<String, (String, Instant)>, // Cache for network_id or other reusable data
  pub cache_ttl: Duration,                       /* Cache time-to-live (network_id is refreshed after this time) */
  pub cache_tx_size: usize,                      // Cache limit for last n transactions submitted
  /// Optional trustless backend: when set, live-state endpoints (mempool, frontier
  /// balance, submit) are served from the mina-light-node instead of the GraphQL
  /// daemon. See [`LightNodeClient`].
  pub light_node: Option<LightNodeClient>,
}
