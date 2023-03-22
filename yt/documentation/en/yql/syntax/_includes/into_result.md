# INTO RESULT

Lets you specify the user label for [SELECT](../select.md), [PROCESS](../process.md), or [REDUCE](../reduce.md). It can't be used along with [DISCARD](../discard.md).

**Examples:**

```yql
SELECT 1 INTO RESULT foo;
```

```yql
SELECT * FROM
my_table
WHERE value % 2 == 0
INTO RESULT `Name of the result`.
```
