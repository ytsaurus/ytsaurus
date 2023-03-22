## Listing the contents of a directory in a cluster {#folder}

Specified as the `FOLDER` function in [FROM](#from).

Arguments:

1. The path to the directory.
2. An optional string with a list of meta attributes separated by a semicolon.

The result is a table with three fixed columns:

1. **Path** (`String`): The full name of the table.
2. **Type** (`String`): The node type (table, map_node, file, document, and others).
3. **Attributes** (`Yson`) is a Yson dictionary with meta attributes ordered in the second argument.

Recommendations for use:

* To get only the list of tables, you must add `...WHERE Type == "table"`. Then you can add more conditions if you want using the `AGGREGATE_LIST` aggregate function from the Path column to get only the list of paths and pass them to [EACH](#each).
* Because the Path column is returned in the same format as the `TablePath()` function's output, you can use it for the table's JOIN attributes for its rows.
* We recommend that you work with the Attributes column using [Yson UDF](../../../udf/list/yson.md).

{% note warning "Attention!" %}

Use FOLDER with attributes containing large values with caution (`schema` could be one of those). The query containing FOLDER in a folder with a large number of tables and a heavy attribute may cause a severe load on the wizard{{product-name}}.

{% endnote %}

**Examples:**

```yql
USE hahn;

$table_paths = (
    SELECT AGGREGATE_LIST(Path)
    FROM FOLDER("my_folder", "schema;row_count")
    WHERE
        Type = "table" AND
        Yson::GetLength(Attributes.schema) > 0 AND
        Yson::LookupInt64(Attributes, "row_count") > 0
);

SELECT COUNT(*) FROM EACH($table_paths);
```
