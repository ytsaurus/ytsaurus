    id
   ,name
   ,first_appeared
FROM [$nomenclature]
WHERE first_appeared BETWEEN 1167598800000000 AND 1199134800000000
ORDER BY
    first_appeared
LIMIT 10
