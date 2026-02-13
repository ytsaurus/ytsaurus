-- Returns top 10 products sorted by their first appearance date (descending).
-- Demonstrates ORDER BY with LIMIT for fetching most recent entries.

SELECT
    n.id
   ,n.name
   ,n.first_appeared
FROM '$nomenclature' n
ORDER BY
    n.first_appeared DESC
LIMIT 10;
