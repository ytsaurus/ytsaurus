---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/drop_table.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/drop_table.md
---
# DROP TABLE

Удаляет указанную таблицу.  Таблица по имени ищется в базе данных, заданной оператором [USE](../use.md).

Если таблицы с таким именем не существует, возвращается ошибка. 

**Примеры:**

``` yql
DROP TABLE my_table;
```
