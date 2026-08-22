SELECT
  zc.id,
  zc.memo,
  zc.hash,
  pk_fee_payer.value AS fee_payer,
  zfpb.fee,
  zfpb.valid_until,
  zfpb.nonce,
  bzc.sequence_no,
  bzc.status AS "status: TransactionStatus",
  b.state_hash,
  b.height,
  b.timestamp,
  bzc.block_id,
  cast(0 AS BIGINT) AS total_count,
  ARRAY(
    SELECT
      unnest(zauf.failures)
    FROM
      zkapp_account_update_failures AS zauf
    WHERE
      zauf.id=ANY (bzc.failure_reasons_ids)
  ) AS failure_reasons,
  zaub.balance_change AS "balance_change?",
  ac.creation_fee AS "creation_fee?",
  pk_update_body.value AS "pk_update_body?",
  token_update_body.value AS "token?"
FROM
  blocks_zkapp_commands AS bzc
  INNER JOIN zkapp_commands AS zc ON bzc.zkapp_command_id=zc.id
  INNER JOIN zkapp_fee_payer_body AS zfpb ON zc.zkapp_fee_payer_body_id=zfpb.id
  INNER JOIN public_keys AS pk_fee_payer ON zfpb.public_key_id=pk_fee_payer.id
  INNER JOIN blocks AS b ON bzc.block_id=b.id
  /* An account update id can appear more than once in zkapp_account_updates_ids, and the ledger
     applies it once per occurrence. Enumerating the array positionally yields one row per applied
     update; matching with `= ANY (...)` collapses repeats and silently drops their balance
     changes. */
  LEFT JOIN LATERAL unnest(zc.zkapp_account_updates_ids) WITH ORDINALITY AS au_ref (au_id, au_ord) ON TRUE
  LEFT JOIN zkapp_account_update AS zau ON zau.id=au_ref.au_id
  LEFT JOIN zkapp_account_update_body AS zaub ON zau.body_id=zaub.id
  LEFT JOIN account_identifiers AS ai_update_body ON zaub.account_identifier_id=ai_update_body.id
  LEFT JOIN public_keys AS pk_update_body ON ai_update_body.public_key_id=pk_update_body.id
  LEFT JOIN tokens AS token_update_body ON ai_update_body.token_id=token_update_body.id
  /* The account creation fee charged to an account this update created.

     Mina charges it to the created account itself only when the update carries
     implicit_account_creation_fee, by subtracting it from that update's own balance_change --
     so the archive's recorded change is gross of the fee and an operation is needed to bring the
     account back to what the ledger actually credited. Without the flag the fee comes out of the
     command's excess instead, funded by the command's own negative balance changes, which are
     already emitted as zkapp_balance_update operations.

     The ledger rejects an implicit fee on a non-default token (Cannot_pay_creation_fee_in_token),
     so for an applied command such an update is always a MINA update and survives this query's
     token filter.

     A created account is billed once per block, so where several applied updates in the block
     could claim it -- including repeats of the same update -- only the earliest position does. */
  LEFT JOIN accounts_created AS ac ON ac.block_id=bzc.block_id
  AND ac.account_identifier_id=zaub.account_identifier_id
  AND bzc.status='applied'
  AND zaub.implicit_account_creation_fee
  AND NOT EXISTS (
    SELECT
      1
    FROM
      blocks_zkapp_commands AS bzc2
      INNER JOIN zkapp_commands AS zc2 ON zc2.id=bzc2.zkapp_command_id
      CROSS JOIN LATERAL unnest(zc2.zkapp_account_updates_ids) WITH ORDINALITY AS au_ref2 (au_id, au_ord)
      INNER JOIN zkapp_account_update AS zau2 ON zau2.id=au_ref2.au_id
      INNER JOIN zkapp_account_update_body AS zaub2 ON zaub2.id=zau2.body_id
    WHERE
      bzc2.block_id=bzc.block_id
      AND bzc2.status='applied'
      AND zaub2.implicit_account_creation_fee
      AND zaub2.account_identifier_id=zaub.account_identifier_id
      AND (bzc2.sequence_no, zc2.id, au_ref2.au_ord)<(bzc.sequence_no, zc.id, au_ref.au_ord)
  )
WHERE
  bzc.block_id=$1
  AND (
    token_update_body.value=$2
    OR token_update_body.id IS NULL
  )
ORDER BY
  zc.id,
  bzc.sequence_no,
  au_ref.au_ord
