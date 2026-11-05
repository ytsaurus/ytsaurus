-- The example demonstrates how table views work.
-- The views can be two types: linked to specific tables or independent.
-- Both of the views are non-materialized. It means that they are substituted into the calculation graph at each use.

-- Here we use the "plain" view linked to the "$nomenclature" table.
-- The SQL text of this view can be seen on the "User Attributes" tab of the table.
-- It is stored in the "$nomenclature/@_yql_view_plain" attribute.
-- Adding a new linked view is accomplished by adding "_yql_view_{name}" attribute to the table.
SELECT
    * WITHOUT id, is_rx -- syntax for excluding columns from the result
FROM `$nomenclature` VIEW plain
WHERE
    NOT is_rx
    AND max_temperature - min_temperature < 20;


-- Here we access a set of tables with bound views.
-- The views are present on each of the tables.
SELECT
    table_date,
    sum(quantity) AS total
FROM -- The view name is specified by the last parameter of "PARTITIONS" function.
  PARTITIONS('$orders', '${table_date:Date}', 'plain')
WHERE
    "Good" IN quality
GROUP BY
    table_date
ORDER BY
    table_date;

-- Here is used the view not tied to a specific table.
SELECT
    origin,
    sum(price * quantity) AS total
FROM `$joint`
GROUP BY origin
ORDER BY total DESC;
-- Such independent views can be created through SQL.
/* The view used above was created by the following query:
$create_joint_view*/
