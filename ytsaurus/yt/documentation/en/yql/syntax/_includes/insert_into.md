# INSERT INTO
Adds rows to the table.  If the target table already exists and is not sorted, the operation `INSERT INTO` adds rows at the end of the table. For a sorted table, YQL tries to preserve sorting by starting a sorted merge.

Search for the table by name in the database specified by the [USE](../use.md) operator.

`INSERT INTO` lets you perform the following operations:

* Adding constant values using [`VALUES`](../values.md).

   ```sql
   INSERT INTO my_table (Key1, Key2, Value1, Value2)
   VALUES (345987,'ydb', 'Apple region', 1414);
   COMMIT;
   ```

   ```sql
   INSERT INTO my_table (key, value)
   VALUES ("foo", 1), ("bar", 2);
   ```

* Saving the `SELECT` result.

   ```sql
   INSERT INTO my_table
   SELECT Key AS Key1, "Empty" AS Key2, Value AS Value1
   FROM my_table1;
   ```

