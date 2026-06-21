use std::time::Duration;

use anyhow::Result;
use clap::{Args, Parser};
use coinbase_mesh::models::BlockIdentifier;
use cynic::QueryBuilder;
use dashmap::DashMap;
use sqlx::postgres::PgPoolOptions;

use crate::{
  graphql::{self, GraphQLClient},
  util::default_mina_proxy_url,
  MinaMesh, MinaMeshError,
};

#[derive(Debug, Args)]
pub struct MinaMeshConfig {
  /// The URL of the Mina GraphQL
  #[arg(long, env = "MINAMESH_PROXY_URL", default_value_t = default_mina_proxy_url())]
  pub proxy_url: String,

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
    let light_node = self.light_node_url.as_ref().map(|url| {
      tracing::info!("Trustless light-node backend enabled at {url}");
      crate::LightNodeClient::new(url.to_owned())
    });
    let indexer = self.indexer_url.as_ref().map(|url| {
      tracing::info!("Trustless indexer backend enabled at {url}");
      crate::IndexerClient::new(url.to_owned())
    });
    // The archive Postgres is required only when the indexer isn't serving history.
    if indexer.is_none() && self.archive_database_url.is_none() {
      return Err(MinaMeshError::Exception(
        "set MINAMESH_INDEXER_URL or MINAMESH_ARCHIVE_DATABASE_URL (one backs historical reads)".to_string(),
      ));
    }
    let pg_pool = match &self.archive_database_url {
      Some(url) => Some(
        PgPoolOptions::new()
          .max_connections(self.max_db_pool_size)
          .min_connections(0)
          .idle_timeout(Duration::from_secs(self.db_pool_idle_timeout))
          .connect(url.as_str())
          .await?,
      ),
      None => None,
    };
    // `mina:<network>` — the Rosetta network id this server validates against.
    let network_id = format!("mina:{}", self.network);
    let graphql_client = GraphQLClient::new(self.proxy_url.to_owned());

    // Genesis identifier: from the indexer in trustless mode (no daemon), else the daemon.
    let genesis_block_identifier = if let Some(indexer) = &indexer {
      // The indexer may still be starting; retry briefly for its rooted genesis (oldest).
      let mut last = None;
      let mut found = None;
      for _ in 0..30 {
        match indexer.oldest().await {
          Ok(g) => {
            found = Some(BlockIdentifier::new(g.block_height as i64, g.state_hash));
            break;
          }
          Err(e) => {
            last = Some(e);
            tokio::time::sleep(Duration::from_secs(2)).await;
          }
        }
      }
      found.ok_or_else(|| {
        MinaMeshError::Exception(format!("indexer not reachable for genesis identifier: {last:?}"))
      })?
    } else {
      if self.proxy_url.is_empty() {
        return Err(MinaMeshError::GraphqlUriNotSet);
      }
      tracing::info!("Connecting to Mina GraphQL endpoint at {}", self.proxy_url);
      let res = graphql_client.send(graphql::QueryGenesisBlockIdentifier::build(())).await?;
      let block_height = res.genesis_block.protocol_state.consensus_state.block_height.0.parse::<i64>()?;
      let state_hash = res.genesis_block.state_hash.0.clone();
      BlockIdentifier::new(block_height, state_hash)
    };
    tracing::info!("network {network_id}, genesis {genesis_block_identifier:?}");

    Ok(MinaMesh {
      graphql_client,
      network_id,
      pg_pool,
      genesis_block_identifier,
      search_tx_optimized: self.use_search_tx_optimizations,
      cache: DashMap::new(),
      cache_ttl: Duration::from_secs(300),
      cache_tx_size: 100, // Cache limit for last n transactions submitted
      light_node,
      indexer,
    })
  }
}
