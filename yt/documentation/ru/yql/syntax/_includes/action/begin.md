---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/action/begin.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/action/begin.md
---
## BEGIN .. END DO {#begin}

Выполнение действия без его объявления (анонимное действие).

**Синтаксис**
1. `BEGIN`;
1. Список выражений верхнего уровня;
1. `END DO`.

Анонимное действие не может содержать параметров.

**Пример**

```
DO BEGIN
    SELECT 1;
    SELECT 2  -- здесь и в предыдущем примере ';' перед END можно не ставить
END DO
```