-- As with maybe_account_balance_info.sql, the addressable blocks are the canonical chain plus the
-- single pending branch descending from the best tip -- not every pending block at a given height.
-- Resolving a bare height during a fork would otherwise return an arbitrary one of the competing
-- branches.
WITH RECURSIVE
  pending_chain AS (
    (
      SELECT
        id,
        parent_id,
        height,
        state_hash,
        global_slot_since_genesis,
        chain_status
      FROM
        blocks
      WHERE
        height=(
          SELECT
            max(height)
          FROM
            blocks
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
      b.state_hash,
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
      height,
      state_hash,
      global_slot_since_genesis
    FROM
      pending_chain
    UNION ALL
    SELECT
      height,
      state_hash,
      global_slot_since_genesis
    FROM
      blocks
    WHERE
      chain_status='canonical'
  )
SELECT DISTINCT
  height,
  state_hash,
  global_slot_since_genesis
FROM
  full_chain
WHERE
  (
    height=$1
    OR $1 IS NULL
  )
  AND (
    state_hash=$2
    OR $2 IS NULL
  )
