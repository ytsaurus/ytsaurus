-- Filters rows where is_rx is true (e.g., prescription drugs).
-- Demonstrates basic WHERE filtering for boolean fields.

SELECT
    n.id
   ,n.name
   ,n.first_appeared
FROM '$nomenclature' n
WHERE n.is_rx = true;
