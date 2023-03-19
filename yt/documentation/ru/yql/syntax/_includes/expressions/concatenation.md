---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/concatenation.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/expressions/concatenation.md
---
## Конкатенация строк {#concatenation}

Выполняется через бинарный оператор `||`.

Как и у других бинарных операторов, если в данных с одной из сторон оказался `NULL`, то и результат будет `NULL`.

Не следует путать этот оператор с логическим «или», в SQL оно обозначается ключевым словом `OR`. Также не стоит пытаться делать конкатенацию через `+`.

**Примеры**

```sql
SELECT "fo" || "o";
```
