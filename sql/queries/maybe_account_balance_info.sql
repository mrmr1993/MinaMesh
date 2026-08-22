-- The set of blocks a balance may be read from is the canonical chain plus the *single* pending
-- branch that descends from the current best tip. Selecting pending blocks by height alone would
-- admit every competing branch during a fork, and the LIMIT 1 below would then pick among them
-- arbitrarily -- so a balance could be served from a branch about to be orphaned.
--
-- This mirrors the OCaml implementation's `Balance_from_last_relevant_command.query_pending`,
-- except that the branch is seeded from the tip the daemon reports as best ($4) when one is
-- available. Choosing it here by (timestamp, state_hash) is not Mina's consensus rule; the
-- fallback below keeps that behaviour for when the daemon cannot be reached.
WITH RECURSIVE
  pending_chain AS (
    (
      SELECT
        id,
        parent_id,
        height,
        global_slot_since_genesis,
        chain_status
      FROM
        blocks
      WHERE
        (
          $4::text IS NOT NULL
          AND state_hash=$4
        )
        OR (
          $4::text IS NULL
          AND height=(
            SELECT
              max(height)
            FROM
              blocks
          )
        )
      ORDER BY
        TIMESTAMP ASC,
        state_hash ASC
      LIMIT
        1
    )
    UNION ALL
    SELECT
      b.id,
      b.parent_id,
      b.height,
      b.global_slot_since_genesis,
      b.chain_status
    FROM
      blocks b
      INNER JOIN pending_chain ON b.id=pending_chain.parent_id
      AND pending_chain.id<>pending_chain.parent_id
      AND pending_chain.chain_status<>'canonical'
  ),
  full_chain AS (
    SELECT
      id,
      height,
      global_slot_since_genesis
    FROM
      pending_chain
    UNION ALL
    SELECT
      id,
      height,
      global_slot_since_genesis
    FROM
      blocks
    WHERE
      chain_status='canonical'
  )
SELECT DISTINCT
  fc.height,
  fc.global_slot_since_genesis AS block_global_slot_since_genesis,
  balance,
  nonce,
  timing_id,
  t.value AS token_id
FROM
  full_chain fc
  INNER JOIN accounts_accessed ac ON ac.block_id=fc.id
  INNER JOIN account_identifiers ai ON ai.id=ac.account_identifier_id
  INNER JOIN public_keys pks ON ai.public_key_id=pks.id
  INNER JOIN tokens t ON ai.token_id=t.id
WHERE
  pks.value=$1
  AND fc.height<=$2
  AND t.value=$3
ORDER BY
  fc.height DESC
LIMIT
  1
