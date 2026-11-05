CREATE VIEW `$joint` AS
DO BEGIN
SELECT * WITHOUT n.id, p.nomenclature_id, p.date
FROM RANGE('$orders','','','','plain') AS o
INNER JOIN `$nomenclature` VIEW plain AS n
ON o.nomenclature_id = n.id
INNER JOIN `$price` AS p
ON p.nomenclature_id = n.id AND p.date = o.date
END DO;
