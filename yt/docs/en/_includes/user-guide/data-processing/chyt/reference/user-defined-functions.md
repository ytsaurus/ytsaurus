# User-defined SQL functions

## Creating and deleting SQL UDFs { #create_delete_udf }

[CREATE FUNCTION](https://clickhouse.com/docs/en/sql-reference/statements/create/function): Creates a function from a lambda expression. Functions created this way are available to all users within the used clique, but not outside it.
```sql
CREATE FUNCTION linearEquation AS (x, k, b) -> k*x + b;
SELECT number, linearEquation(number, 2, 1) FROM numbers(3);
```

[DROP FUNCTION](https://clickhouse.com/docs/en/sql-reference/statements/drop#drop-function): Deletes a user-defined SQL function.
```sql
DROP FUNCTION linearEquation;
```

Both commands require the `Manage` permission for the clique.

## Using functions in queries { #udf_usage }
UDFs are stored in the clique state, so you don't have to recreate them in every query. You can call a UDF in the same way as any built-in ClickHouse function.

If someone recreates a UDF currently in use while a query is being executed, the calls to this function may use different implementations. There is no guaranteed way to know which one will be selected on each clique instance.

## Listing all SQL UDFs in a clique { #get_all_udfs }
```sql
SELECT name, create_query FROM system.functions
WHERE origin = 'SQLUserDefined'
```
Result:
|name            |create_query |
| -------------- | ----------- |
| linearEquation | CREATE FUNCTION linearEquation AS (x, k, b) -> ((k * x) + b) |
