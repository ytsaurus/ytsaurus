    is_rx
   ,sum(1)
   ,min(first_appeared)
FROM [$nomenclature]
GROUP BY
    is_rx
