# Verifying trustless indexer responses

**Status:** design / roadmap. **Scope:** tiers 0–2 — make MinaMesh re-verify the indexer's responses
so the Rosetta output it serves is cryptographically trustworthy even when the indexer is hosted by a
third party. Pairs with `mina-indexer/docs/trustless-responses.md` (the producing side).

## Goal

A "verified mode" (`MINAMESH_VERIFY=1`, opt-in) in which MinaMesh **never serves indexer data it
hasn't checked**. With it on, the operator can point `MINAMESH_INDEXER_URL` at an untrusted/shared
indexer and still hand exchanges trustworthy `/block`, `/search/transactions`, and (within the
finality window) `/account/balance`.

Trust model: the indexer is untrusted. The only trusted inputs are (a) the hardcoded genesis, (b) the
recursive-SNARK verifier, and (c) **our own light node's verified tip** as the canonicity/finality
anchor (MinaMesh already has `MINAMESH_LIGHT_NODE_URL`).

## Prerequisite: add the `mina-verify` dependency

MinaMesh does **not** currently depend on `mina-verify`. Add it (same git rev the light node pins, so
the `mina-p2p-messages` graph unifies). We need: `verify_block`, `verify_account_inclusion`,
`ledger_root`, `MerklePath`, the block/`Account` types. This is the bulk of the new dependency
surface; the rest is orchestration.

## What changes, by endpoint

For each, MinaMesh requests the response **with the proof envelope** (see the indexer doc) and, before
building the Rosetta object, runs:

### `/block`
1. Parse `proof.block_binprot`; `verifier.verify_block(&block)` — reject on `false`.
2. **Canonicity:** walk `proof.canonicity.parent_chain` from `anchor_state_hash` (cross-checked
   against the light node's verified tip) to the target by `previous_state_hash` links; or, if the
   target is ≥ `k` (290) below the verified tip, accept by finality. Reject otherwise.
3. Build the Rosetta block **from the verified block**, not from the indexer's parsed JSON. The
   existing `block_from_indexer` mapping moves to operate on the verified block.

### `/account/balance` at a finalized `block_identifier`
1. Verify `proof.account.ledger_root_block` as in `/block`.
2. `verify_account_inclusion(merkle_path, leaf)` ⇒ a root; assert it equals the verified block's
   `blockchain_state` ledger root (the **binding** check). Reject on mismatch.
3. Assert the height is within the finality window (≤ verified tip − served depth); otherwise return
   a clear "outside verifiable window" error rather than an unverifiable balance.
4. Serve balance/nonce from the **verified leaf**.

### `/search/transactions`
Verify the containing block; assert the tx is present in the verified block's `staged_ledger_diff`.

## Behavioural rules (these *are* the security)
- **Fail closed.** In verified mode, a missing/invalid proof ⇒ error, never fall through to
  unverified indexer JSON. There must be no code path that serves indexer-sourced data without a
  passing verification.
- **Bind everything to one verified block.** An account proof is only as good as the assertion that
  its Merkle root is the root of the block whose proof we checked (step 2 above). This binding is the
  subtle bug magnet — review it hard.
- **Anchor canonicity to our own light node**, not to anything the indexer says. The indexer can lie
  about which chain is canonical; the light node can't (it verifies). `anchor_state_hash` must be
  reconciled against the light node, not trusted from the envelope.

## Out of scope
- Historical `/account/balance` **older than the finality window** — surfaced as an explicit error in
  verified mode (the data needs retained historical ledger trees the indexer isn't serving in
  tiers 0–2). Unverified (today's) mode is unchanged.

## Review surface (small)
No new crypto — this orchestrates `mina-verify`. The review targets: (1) the canonicity anchor,
(2) the fail-closed property, (3) the account↔block root binding. Days, not a formal audit; write the
one-page threat model ("what can a malicious indexer do, where is it caught") alongside.
