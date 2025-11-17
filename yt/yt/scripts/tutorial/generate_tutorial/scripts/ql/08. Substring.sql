    sum(1) as count
FROM [$nomenclature]
WHERE is_substr("statin", name)
GROUP BY 1
