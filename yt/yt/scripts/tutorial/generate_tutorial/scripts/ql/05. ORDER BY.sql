-- Fetches a limited set of products ordered by first_appeared timestamp.
-- Timestamps are given in microseconds here.

    id
   ,name
   ,first_appeared
FROM [$nomenclature]
WHERE first_appeared BETWEEN 1167598800000000 AND 1199134800000000
ORDER BY
    first_appeared
LIMIT 10
