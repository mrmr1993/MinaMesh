-- Which of the daemon's candidate tips the archive actually holds. Ordered by the caller's
-- preference, which is tip-first, so the first row is the best usable seed.
SELECT
  t.state_hash
FROM
  unnest($1::text[]) WITH ORDINALITY AS t (state_hash, ord)
  INNER JOIN blocks b ON b.state_hash=t.state_hash
ORDER BY
  t.ord ASC
LIMIT
  1
