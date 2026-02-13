-- Counts total number of products within a certain time range.
-- GROUP BY 1 is shorthand for grouping by the first column (here, a constant),
-- count all rows that match the condition.

    sum(1) as count
FROM [$nomenclature]
WHERE first_appeared BETWEEN 1167598800000000 AND 1199134800000000
GROUP BY 1
