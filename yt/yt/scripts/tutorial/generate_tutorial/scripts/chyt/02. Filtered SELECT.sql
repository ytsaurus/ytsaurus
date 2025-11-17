SELECT
    n.id
   ,n.name
   ,n.first_appeared
FROM '$nomenclature' n
WHERE n.is_rx = true;
