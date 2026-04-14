-- Groups products by is_rx flag and computes count and earliest appearance date.
-- Useful for comparing prescription vs non-prescription drug trends.

    is_rx
   ,sum(1)
   ,min(first_appeared)
FROM [$nomenclature]
GROUP BY
    is_rx
