mod api;
mod archive;
mod archive_node_api;
mod commands;
mod config;
mod create_router;
mod error;
mod graphql;
mod indexer;
mod light_node;
pub mod memo;
mod node;
mod playground;
mod roinput;
pub mod signer_utils;
pub mod test;
mod transaction_operations;
mod types;
pub mod util;

use std::time::{Duration, Instant};

pub use archive::*;
pub use archive_node_api::*;
pub use coinbase_mesh::models;
use coinbase_mesh::models::BlockIdentifier;
pub use commands::*;
pub use config::*;
pub use create_router::create_router;
use dashmap::DashMap;
pub use error::*;
pub use indexer::*;
pub use light_node::*;
pub use node::*;
pub(crate) use roinput::*;
pub use transaction_operations::*;
pub use types::*;
pub struct MinaMesh {
  /// The live node, behind one trait. In full mode this is a [`DaemonBackend`] (the only
  /// holder of a `GraphQLClient`); in trustless mode a [`LightNodeBackend`]. There is **no**
  /// ambient daemon client to fall through to — any daemon use must go through this adapter,
  /// which only exists in full mode, so a trustless deployment can never silently hit a
  /// public daemon (the old `proxy_url`-defaults-to-mainnet footgun).
  pub node: Box<dyn MinaNode>,
  /// The history axis, behind one trait: a trustless [`IndexerArchive`] or a trusted
  /// [`PostgresArchive`]. This collapses the old per-handler `if let Some(indexer) { … } else
  /// { …Postgres… }` forks into one selected adapter.
  pub archive: Box<dyn MinaArchive>,
  /// The Rosetta network id this server serves, `mina:<network>`. In trustless mode it's the
  /// source of truth for network validation / `/network/list` (no daemon query needed).
  pub network_id: String,
  pub genesis_block_identifier: BlockIdentifier,
  pub cache: DashMap<String, (String, Instant)>, // Cache for network_id or other reusable data
  pub cache_ttl: Duration,                       /* Cache time-to-live (network_id is refreshed after this time) */
  pub cache_tx_size: usize,                      // Cache limit for last n transactions submitted
}
