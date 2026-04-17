# CREATE VIEW

`CREATE VIEW` creates an independent [view](select/view.md).

YQL over {{product-name}} supports two types of views: independent views, which can request to arbitrary tables within the cluster, and views that are linked to a specific table.

{% if audience == "internal" %}

For more details about linked views and how views are stored in {{product-name}}, see the [section](../misc/schema.md#yql_view).

{% endif %}

## Availability

`CREATE VIEW` is available starting from the language version [2025.05](../changelog/2025.05.md).

## Syntax
```yql
CREATE VIEW [IF NOT EXISTS] [cluster.]`//path/to/view` AS
DO BEGIN
<top_level_statements>
END DO
```

This query will create an independent view on the `cluster` (or on the current cluster if not specified) at the path `//path/to/view`. The view consists of the block of commands `<top_level_statements>`.

The `<top_level_statements>` can include only commands that are allowed in [DEFINE SUBQUERY](subquery.md#define-subquery). The `USE` command or explicit cluster specification for tables cannot be used — within a view, you can only request to tables located on the same cluster as the view itself. Unlike `DEFINE SUBQUERY`, commands in `CREATE VIEW` must not use any values from outside the `CREATE VIEW` body.

If any object already exists at the path `//path/to/view`, the view creation will fail. If the `IF NOT EXISTS` modifier is specified, no error is raised, but the `CREATE VIEW` command does not create a view (completes with no result).

{% note warning "Attention" %}

Creating a view and using it within the same YQL-query is not supported yet.
When creating views, please note that the [language version](../changelog/index.md#langver-desc) for the view will be determined by the query in which the view is used, not by the query in which it was created. The same rule applies to [libraries](export_import.md).

{% endnote %}

## Example

```yql
USE cluster;
$now = CurrentUtcDate();

CREATE VIEW `//home/yql/tutorial/users_with_birth_year` AS
DO BEGIN

$now = CurrentUtcDate(); -- cannot use named expressions from outside

$year_of_birth = ($age) -> (DateTime::GetYear(DateTime::ShiftYears($now, -(cast($age as Int32) ?? 0))));

SELECT $year_of_birth(age) AS birth_year, t.* WITHOUT age FROM `//home/yql/tutorial/users` AS t;

END DO;
```

For information on how to use the created view, see the section [SELECT VIEW](select/view.md).

## See also

* [DROP VIEW](drop_view.md)
