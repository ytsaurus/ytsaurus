
## TableRow, JoinTableRow {#tablerow}

Getting the entire table row as a structure. No arguments. `JoinTableRow` in case of `JOIN` always returns a structure with table prefixes.

**Signature**
```
TableRow()->Struct
```

**Example**
```yql
SELECT TableRow() FROM my_table;
```
