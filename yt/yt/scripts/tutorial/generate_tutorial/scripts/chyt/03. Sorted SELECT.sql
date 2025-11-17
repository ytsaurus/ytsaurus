SELECT
    n.id
   ,n.name
   ,n.first_appeared
FROM '$nomenclature' n
ORDER BY
    n.first_appeared DESC
LIMIT 10;
