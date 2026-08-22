-- The aggregated tables added by migration 1 are maintained by AFTER INSERT triggers only, so
-- rows survive the deletion of the block they belong to. The archive deletes blocks routinely --
-- `mina-archive prune`, the `delete_older_than` option, and the ON DELETE CASCADE from `blocks`
-- to the join tables -- so without a delete path the aggregated tables only ever grow, and
-- /search/transactions keeps returning transactions from blocks the archive no longer holds.
--
-- Row-level triggers fire for cascaded deletes too, so dropping a block propagates through the
-- join tables and into these.
CREATE OR REPLACE FUNCTION remove_from_user_commands_aggregated () returns trigger AS $$
BEGIN
  DELETE FROM user_commands_aggregated
  WHERE user_command_id = OLD.user_command_id
    AND block_id = OLD.block_id
    AND sequence_no = OLD.sequence_no;
  RETURN OLD;
END;
$$ language plpgsql;

-- NEXT --
CREATE OR REPLACE trigger trigger_remove_from_user_commands_aggregated
AFTER delete ON blocks_user_commands FOR each ROW
EXECUTE function remove_from_user_commands_aggregated ();

-- NEXT --
CREATE OR REPLACE FUNCTION remove_from_internal_commands_aggregated () returns trigger AS $$
BEGIN
  DELETE FROM internal_commands_aggregated
  WHERE id = OLD.internal_command_id
    AND block_id = OLD.block_id
    AND sequence_no = OLD.sequence_no
    AND secondary_sequence_no = OLD.secondary_sequence_no;
  RETURN OLD;
END;
$$ language plpgsql;

-- NEXT --
CREATE OR REPLACE trigger trigger_remove_from_internal_commands_aggregated
AFTER delete ON blocks_internal_commands FOR each ROW
EXECUTE function remove_from_internal_commands_aggregated ();

-- NEXT --
CREATE OR REPLACE FUNCTION remove_from_zkapp_commands_aggregated () returns trigger AS $$
BEGIN
  DELETE FROM zkapp_commands_aggregated
  WHERE id = OLD.zkapp_command_id
    AND block_id = OLD.block_id
    AND sequence_no = OLD.sequence_no;
  RETURN OLD;
END;
$$ language plpgsql;

-- NEXT --
CREATE OR REPLACE trigger trigger_remove_from_zkapp_commands_aggregated
AFTER delete ON blocks_zkapp_commands FOR each ROW
EXECUTE function remove_from_zkapp_commands_aggregated ();

-- NEXT --
-- Anything already stranded by an insert-only history is removed here, so that applying this
-- migration leaves the aggregated tables consistent rather than merely stopping the bleeding.
DELETE FROM user_commands_aggregated a
WHERE NOT EXISTS (
  SELECT 1 FROM blocks_user_commands j
  WHERE j.user_command_id = a.user_command_id
    AND j.block_id = a.block_id
    AND j.sequence_no = a.sequence_no
);

-- NEXT --
DELETE FROM internal_commands_aggregated a
WHERE NOT EXISTS (
  SELECT 1 FROM blocks_internal_commands j
  WHERE j.internal_command_id = a.id
    AND j.block_id = a.block_id
    AND j.sequence_no = a.sequence_no
    AND j.secondary_sequence_no = a.secondary_sequence_no
);

-- NEXT --
DELETE FROM zkapp_commands_aggregated a
WHERE NOT EXISTS (
  SELECT 1 FROM blocks_zkapp_commands j
  WHERE j.zkapp_command_id = a.id
    AND j.block_id = a.block_id
    AND j.sequence_no = a.sequence_no
);
