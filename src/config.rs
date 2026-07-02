use std::time::Duration;

use anyhow::Result;
use clap::{Args, Parser};
use coinbase_mesh::models::BlockIdentifier;
use cynic::QueryBuilder;
use dashmap::DashMap;
use sqlx::postgres::PgPoolOptions;

use crate::{
  graphql::{self, GraphQLClient},
  MinaMesh, MinaMeshError,
};

#[derive(Debug, Args)]
pub struct MinaMeshConfig {
  /// The URL of the Mina GraphQL daemon. Required only in **full mode** (no light node):
  /// it backs the trusted [`DaemonBackend`]. There is intentionally **no default** — the old
  /// default (`https://mainnet.minaprotocol.network/graphql`) was a footgun: in trustless mode
  /// any accidental daemon use silently hit public mainnet. Now, with no light node and no
  /// `proxy_url`, startup fails loudly instead.
  #[arg(long, env = "MINAMESH_PROXY_URL")]
  pub proxy_url: Option<String>,

  /// The URL of the Archive Database. Optional when `MINAMESH_INDEXER_URL` is set —
  /// historical reads then come from the trustless mina-indexer instead of Postgres.
  #[arg(long, env = "MINAMESH_ARCHIVE_DATABASE_URL")]
  pub archive_database_url: Option<String>,

  /// The maximum number of concurrent connections allowed in the Archive
  /// Database connection pool.
  #[arg(long, env = "MINAMESH_MAX_DB_POOL_SIZE", default_value_t = 128)]
  pub max_db_pool_size: u32,

  /// The duration (in seconds) that an unused connection can remain idle in the
  /// pool before being closed.
  #[arg(long, env = "MINAMESH_DB_POOL_IDLE_TIMEOUT", default_value_t = 1)]
  pub db_pool_idle_timeout: u64,

  /// Whether to use optimizations for searching transactions. Requires the
  /// optimizations to be enabled via the `mina-mesh search-tx-optimizations`
  /// command.
  #[arg(long, env = "USE_SEARCH_TX_OPTIMIZATIONS", default_value = "false")]
  pub use_search_tx_optimizations: bool,

  /// Optional URL of a trustless `mina-light-node-server`. When set, live-state
  /// endpoints (mempool, frontier balance, submit) are served from the light node
  /// (proof-anchored reads, peer-to-peer submit) instead of the GraphQL daemon.
  #[arg(long, env = "MINAMESH_LIGHT_NODE_URL")]
  pub light_node_url: Option<String>,

  /// Optional URL of a trustless `mina-indexer` (GraphQL/REST, default :8080). When set,
  /// historical reads (block, historical balance, search, oldest block) are served from
  /// the indexer — which ingests a block only after its SNARK proof verifies — instead of
  /// a Postgres archive. See [`crate::IndexerClient`].
  #[arg(long, env = "MINAMESH_INDEXER_URL")]
  pub indexer_url: Option<String>,

  /// Optional URL of an `Archive-Node-API` GraphQL server. When set (and no indexer is
  /// configured), history reads are served from it instead of a Postgres archive. It is a
  /// **partial** backend: it serves `/block` (by index) + oldest/tip, but not historical
  /// balance, search, or construction nonce. See [`crate::ArchiveNodeApiArchive`].
  #[arg(long, env = "MINAMESH_ARCHIVE_NODE_API_URL")]
  pub archive_node_api_url: Option<String>,

  /// Network name (e.g. `devnet`, `mainnet`). In trustless mode (indexer set) this is the
  /// source of truth for `/network/list` + network validation and the genesis identifier is
  /// taken from the indexer — so no Mina daemon GraphQL (`proxy_url`) is needed at all.
  #[arg(long, env = "MINAMESH_NETWORK", default_value = "devnet")]
  pub network: String,
}

impl MinaMeshConfig {
  pub fn from_env() -> Self {
    dotenv::dotenv().ok();
    return MinaMeshConfigParser::parse().config;

    #[derive(Parser)]
    struct MinaMeshConfigParser {
      #[command(flatten)]
      config: MinaMeshConfig,
    }
  }

  pub async fn to_mina_mesh(self) -> Result<MinaMesh, MinaMeshError> {
    // Select the history backend behind the `MinaArchive` trait (first configured wins):
    //   indexer         ⇒ trustless `IndexerArchive`      (provenance Verified)
    //   archive-node-api ⇒ trusted `ArchiveNodeApiArchive` (provenance TrustedArchive, partial)
    //   archive database ⇒ trusted `PostgresArchive`        (provenance TrustedArchive)
    let archive: Box<dyn crate::MinaArchive> = if let Some(url) = &self.indexer_url {
      tracing::info!("Trustless indexer archive enabled at {url}");
      Box::new(crate::IndexerArchive::new(crate::IndexerClient::new(url.to_owned())))
    } else if let Some(url) = &self.archive_node_api_url {
      tracing::info!("Archive-Node-API archive enabled at {url}");
      Box::new(crate::ArchiveNodeApiArchive::new(crate::ArchiveNodeApiClient::new(url.to_owned())))
    } else {
      let url = self.archive_database_url.as_ref().ok_or_else(|| {
        MinaMeshError::Exception(
          "set MINAMESH_INDEXER_URL, MINAMESH_ARCHIVE_NODE_API_URL, or MINAMESH_ARCHIVE_DATABASE_URL (one backs historical reads)".to_string(),
        )
      })?;
      let pool = PgPoolOptions::new()
        .max_connections(self.max_db_pool_size)
        .min_connections(0)
        .idle_timeout(Duration::from_secs(self.db_pool_idle_timeout))
        .connect(url.as_str())
        .await?;
      tracing::info!("Trusted Postgres archive at {url}");
      Box::new(crate::PostgresArchive::new(pool, self.use_search_tx_optimizations))
    };
    // `mina:<network>` — the Rosetta network id this server validates against.
    let network_id = format!("mina:{}", self.network);

    // Select the live-node backend behind the `MinaNode` trait:
    //   light-node configured  ⇒ trustless `LightNodeBackend` (provenance Verified)
    //   else                   ⇒ trusted `DaemonBackend` over the configured `proxy_url`.
    // The `DaemonBackend` is the *only* holder of a `GraphQLClient`. In trustless mode no such
    // client exists, so there is nothing to silently fall through to a public daemon.
    let (node, daemon_client_for_genesis): (Box<dyn crate::MinaNode>, Option<GraphQLClient>) =
      if let Some(url) = &self.light_node_url {
        tracing::info!("Trustless light-node backend enabled at {url}");
        let backend = crate::LightNodeBackend::new(crate::LightNodeClient::new(url.to_owned()), network_id.clone());
        (Box::new(backend), None)
      } else {
        // Full mode: a daemon is mandatory. No mainnet default — fail loudly if unset.
        let proxy_url = self.proxy_url.clone().filter(|u| !u.is_empty()).ok_or(MinaMeshError::GraphqlUriNotSet)?;
        tracing::info!("Trusted daemon backend at {proxy_url}");
        let client = GraphQLClient::new(proxy_url);
        (Box::new(crate::DaemonBackend::new(client.clone())), Some(client))
      };

    // Genesis identifier: from the standalone archive's oldest block when it backs history
    // without a daemon (indexer or archive-node-api), else from the daemon (Postgres mode).
    let archive_backs_genesis = self.indexer_url.is_some() || self.archive_node_api_url.is_some();
    let genesis_block_identifier = if archive_backs_genesis {
      // The archive may still be starting; retry briefly for its rooted genesis (oldest).
      let mut last = None;
      let mut found = None;
      for _ in 0..30 {
        match archive.oldest_block_identifier().await {
          Ok(g) => {
            found = Some(g);
            break;
          }
          Err(e) => {
            last = Some(e);
            tokio::time::sleep(Duration::from_secs(2)).await;
          }
        }
      }
      found
        .ok_or_else(|| MinaMeshError::Exception(format!("archive not reachable for genesis identifier: {last:?}")))?
    } else {
      // History from Postgres ⇒ genesis from the daemon. Only reachable in full mode, where
      // `daemon_client_for_genesis` is `Some`.
      let client = daemon_client_for_genesis.ok_or(MinaMeshError::GraphqlUriNotSet)?;
      let res = client.send(graphql::QueryGenesisBlockIdentifier::build(())).await?;
      let block_height = res.genesis_block.protocol_state.consensus_state.block_height.0.parse::<i64>()?;
      let state_hash = res.genesis_block.state_hash.0.clone();
      BlockIdentifier::new(block_height, state_hash)
    };
    tracing::info!("network {network_id}, genesis {genesis_block_identifier:?}");

    Ok(MinaMesh {
      node,
      archive,
      network_id,
      genesis_block_identifier,
      cache: DashMap::new(),
      cache_ttl: Duration::from_secs(300),
      cache_tx_size: 100, // Cache limit for last n transactions submitted
    })
  }
}
