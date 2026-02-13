-- Counts how many product names contain the substring "statin".
-- Uses is_substr for efficient substring matching in YQL.
-- Useful for text-based filtering and analysis.

    sum(1) as count
FROM [$nomenclature]
WHERE is_substr("statin", name)
GROUP BY 1
