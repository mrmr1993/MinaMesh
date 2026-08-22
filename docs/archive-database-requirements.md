# Archive database requirements

Mina Mesh reads history from an archive node's PostgreSQL database. Two
properties of that database are easy to miss and both surface as confusing
runtime failures rather than as startup errors.

## The official archive dumps are SERIALIZABLE

The archive dumps published at
`https://storage.googleapis.com/mina-archive-dumps` set the isolation level on
the database itself:

```sql
ALTER DATABASE archive SET default_transaction_isolation TO 'serializable';
```

So every archive restored from an official dump runs every transaction —
including Mina Mesh's read-only queries — under `SERIALIZABLE`, whatever the
server's `default_transaction_isolation` says.

Under `SERIALIZABLE`, PostgreSQL takes **predicate locks** (`SIReadLock`) on the
rows and pages a query reads. Mina Mesh's balance query scans the canonical
chain, which on a mainnet- or devnet-sized archive means roughly 40 predicate
locks per call. The predicate lock table is sized by
`max_pred_locks_per_transaction`, whose default is **64**, and under concurrent
load — several `/account/balance` requests in flight, which is exactly what a
`mesh-cli check:data` run or a busy exchange integration produces — that table
is exhausted.

The failure looks like this, and is reported as non-retriable, so callers see a
hard error rather than a slow one:

```
/account/balance error {Code:1 Message:SQL failure: error returned from database:
out of shared memory ... Details:map[error:... extra:Internal SQL query failed]}
```

The server log carries the diagnosis that the API response does not:

```
ERROR:  out of shared memory
HINT:  You might need to increase "max_pred_locks_per_transaction".
```

**Fix.** Raise the limit, then restart PostgreSQL:

```sql
ALTER SYSTEM SET max_pred_locks_per_transaction = 4096;
```

4096 is comfortable for a devnet-sized archive under `check:data`; the setting
costs shared memory in proportion to `max_connections`, so size it against your
pool rather than copying a number.

Alternatively, if nothing else writes to your archive replica, you can drop the
requirement entirely:

```sql
ALTER DATABASE archive SET default_transaction_isolation TO 'read committed';
```

Mina Mesh only reads, so it does not need serializable snapshots. Do not do this
on a database an archive node is actively writing to without checking what that
node expects.

## The connection pool default exceeds PostgreSQL's default

`MINAMESH_MAX_DB_POOL_SIZE` defaults to **128**, while PostgreSQL's
`max_connections` defaults to **100**. A default Mina Mesh against a default
PostgreSQL can therefore exhaust connections under load.

Either lower the pool:

```sh
export MINAMESH_MAX_DB_POOL_SIZE=32
```

or raise the server, remembering that `max_connections` also multiplies the lock
tables above:

```sql
ALTER SYSTEM SET max_connections = 300;
```

## A quick check

Against a database you intend to serve from:

```sql
SELECT current_setting('max_pred_locks_per_transaction') AS pred_locks,
       current_setting('max_connections')                AS max_connections,
       (SELECT setconfig FROM pg_db_role_setting s
        JOIN pg_database d ON d.oid = s.setdatabase
        WHERE d.datname = current_database())            AS database_overrides;
```

If `database_overrides` mentions `default_transaction_isolation=serializable`
and `pred_locks` is still `64`, you will hit the failure above under load.
