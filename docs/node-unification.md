# Unifying the node interface (light node ⇄ full daemon)

**Goal:** MinaMesh talks to "the node" — live tip, mempool, submit, live account state — through **one
trait**, with the light node and the full daemon as interchangeable adapters. Today these are
scattered as `self.graphql_client.send(...)` (daemon) vs `if let Some(light_node)` branches, which
drift out of sync (that's the `/mempool/transaction` 404 bug: list went through the light node, get
still hit the daemon — which, in trustless mode, silently defaults to *public mainnet*).

**Scope:** the **node** (live) surface only. The **indexer** (historical blocks / accounts / search)
is a *separate* axis and is **not touched** — history handlers keep calling the indexer directly.

## The trait

```rust
#[async_trait]
pub trait MinaNode: Send + Sync {
    async fn network_id(&self) -> Result<String, MinaMeshError>;          // "mina:testnet"
    async fn status(&self) -> Result<NodeStatus, MinaMeshError>;          // best/verified tip + sync
    async fn account(&self, pubkey: &str, token_id: &str)                 // live balance + nonce
        -> Result<NodeAccount, MinaMeshError>;
    async fn mempool(&self) -> Result<Vec<String>, MinaMeshError>;        // pending tx hashes
    async fn mempool_transaction(&self, hash: &str)                       // a single pending tx
        -> Result<Option<NodeUserCommand>, MinaMeshError>;
    async fn submit_payment(&self, p: &SignedPayment) -> Result<String, MinaMeshError>;
    async fn submit_delegation(&self, d: &SignedDelegation) -> Result<String, MinaMeshError>;

    /// How the caller knows these responses are true — must NOT be flattened away.
    fn provenance(&self) -> Provenance;
}

pub enum Provenance {
    Verified,       // light node: SNARK-verified blocks, Merkle-proved balances, signature-checked mempool
    TrustedDaemon,  // full mode: you operate the node
}
```

Neutral MinaMesh types (`NodeStatus`, `NodeAccount`, `NodeUserCommand`, `SignedPayment`,
`SignedDelegation`) — adapters map the backend's native types onto these. `NodeUserCommand` mirrors
the indexer's `IxUserCommand` shape so `generate_operations_user_command` is reused unchanged.

> Genesis and the *oldest* block stay out of this trait: genesis is resolved once at startup, and
> `/network/status`'s oldest comes from the **indexer** (history). Those handlers compose
> node-tip + indexer as they do now.

## Adapters (both already ~covered)

| trait method | `DaemonBackend` → **mina-sdk-rust** | `LightNodeBackend` → light-node client |
|---|---|---|
| `network_id` | `get_network_id` | config / `tip` |
| `status` | `get_daemon_status` | `tip()` |
| `account` | `get_account(pk, token)` | `account(pk)` |
| `mempool` | `get_pooled_user_commands(None)` → hashes | `mempool()` |
| `mempool_transaction` | `get_pooled_user_commands` filtered by hash | **NEW**: `/mempool/tx?hash=` (light node already holds the full `command` in `MempoolView`) |
| `submit_payment` | `send_payment` | `submit(tx_hex)` |
| `submit_delegation` | `send_delegation` | `submit(tx_hex)` |
| `provenance` | `TrustedDaemon` | `Verified` |

`mina-sdk-rust` (`o1-labs/mina-sdk-rust`) is a thin **GraphQL client** — a client *to* a node, not a
node — so it fits `DaemonBackend` without pulling in node infra. It replaces the hand-rolled `cynic`
queries (`QueryNetworkId`, `QueryMempool`, `SendPayment`, …).

## Migration (incremental, no big-bang)
1. Define `MinaNode` + the neutral types + `Provenance`.
2. Wrap the **existing** clients as `LightNodeBackend` / `DaemonBackend` (no behaviour change), put a
   `Box<dyn MinaNode>` on `MinaMesh`, and migrate handlers off `graphql_client` / `light_node` onto it.
   Delete the `if let Some(light_node)` branches. **The `/mempool/transaction` bug is fixed here** —
   it routes through the same backend as `/mempool`. And the silent-mainnet footgun dies: there is no
   ambient daemon client, only the configured adapter.
3. Add the light node's `/mempool/tx` endpoint (decode `MinaBaseUserCommandStableV2` → fields,
   signature-checked) so `LightNodeBackend::mempool_transaction` returns real data.
4. Swap `DaemonBackend`'s internals from `cynic` to `mina-sdk-rust`.

Steps 1–2 are a self-contained MinaMesh change (fixes the bug + removes the footgun). Steps 3–4 are
the light-node endpoint and the SDK swap. The verifiable-indexer proof envelope later rides on the
same `Provenance` contract.
