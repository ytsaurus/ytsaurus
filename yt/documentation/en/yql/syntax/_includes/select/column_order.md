
## Column order in YQL {#orderedcolumns}
The standard SQL is sensitive to the order of columns in projections (that is, in `SELECT`). While the order of columns must be preserved in the query results or when writing data to a new table, some SQL constructs use this order.
This is also true for [UNION ALL](#unionall) and positional [ORDER BY](#orderby) (ORDER BY ordinal).

The column order is ignored in YQL by default:
* The order of columns in the output tables and query results is undefined
* The data scheme of the `UNION ALL` result is output by column names rather than positions

If you enable `PRAGMA OrderedColumns;`, the order of columns is preserved in the query results and is derived from the order of columns in the input tables using the following rules:
* `SELECT`: an explicit column enumeration dictates the result order.
* `SELECT` with an asterisk (`SELECT * FROM ...`) inherits the order from its input.
* the column order after [JOIN](../../join.md): first, the columns from the left side, then the columns from the right side. If the order for one of the sides included in the `JOIN` output is not defined, the order of output columns is not defined either.
* `UNION ALL` order depends on [UNION ALL](#unionall) execution mode.
* the column order for [AS_TABLE](#as_table) is not defined.
