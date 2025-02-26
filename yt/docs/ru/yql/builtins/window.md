
# Список оконных функций в YQL
Синтаксис вызова оконных функций подробно описан в [отдельной статье](../syntax/window.md).


## Агрегатные функции {#aggregate-functions}

Все [агрегатные функции](aggregation.md) также могут использоваться в роли оконных.
В этом случае на каждой строке оказывается результат агрегации, полученный на множестве строк из [рамки окна](../syntax/window.md#frame).

**Примеры:**
``` yql
SELECT
    SUM(int_column) OVER w1 AS running_total,
    SUM(int_column) OVER w2 AS total,
FROM my_table
WINDOW
    w1 AS (ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW),
    w2 AS ();
```


## ROW_NUMBER {#row_number}

Номер строки в рамках [раздела](../syntax/window.md#partition). Без аргументов.

#### Сигнатура
```
ROW_NUMBER()->Uint64
```


#### Примеры
``` yql
SELECT
    ROW_NUMBER() OVER w AS row_num
FROM my_table
WINDOW w AS (ORDER BY key);
```


## LAG / LEAD {#lag-lead}

Доступ к значению из строки [раздела](../syntax/window.md#partition), отстающей (`LAG`) или опережающей (`LEAD`) текущую на фиксированное число. В первом аргументе указывается выражение, к которому необходим доступ, а во втором — отступ в строках. Отступ можно не указывать, по умолчанию используется соседняя строка — предыдущая или следующая, соответственно, то есть подразумевается 1. В строках, для которых нет соседей с заданным расстоянием (например `LAG(expr, 3)` в первой и второй строках раздела), возвращается `NULL`.

#### Сигнатура
```
LEAD(T[,Int32])->T?
LAG(T[,Int32])->T?
```

#### Примеры
``` yql
SELECT
   int_value - LAG(int_value) OVER w AS int_value_diff
FROM my_table
WINDOW w AS (ORDER BY key);
```


## FIRST_VALUE / LAST_VALUE

Доступ к значениям из первой и последней (в порядке `ORDER BY` на окне) строк [рамки окна](../syntax/window.md#frame). Единственный аргумент - выражение, к которому необходим доступ.

Опционально перед `OVER` может указываться дополнительный модификатор `IGNORE NULLS`, который меняет поведение функций на первое или последнее __не пустое__ (то есть не `NULL`) значение среди строк рамки окна. Антоним этого модификатора — `RESPECT NULLS` является поведением по умолчанию и может не указываться.

#### Сигнатура
```
FIRST_VALUE(T)->T?
LAST_VALUE(T)->T?
```

#### Примеры
``` yql
SELECT
   FIRST_VALUE(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```

``` yql
SELECT
   LAST_VALUE(my_column) IGNORE NULLS OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```


## RANK / DENSE_RANK {#rank}

Пронумеровать группы соседних строк [раздела](../syntax/window.md#partition) с одинаковым значением выражения в аргументе. `DENSE_RANK` нумерует группы подряд, а `RANK` — пропускает `(N - 1)` значений, где `N` — число строк в предыдущей группе.

При отсутствии аргумента использует порядок, указанный в секции `ORDER BY` определения окна.
Если аргумент отсутствует и `ORDER BY` не указан, то все строки считаются равными друг другу.

{% note info %}

Возможность передавать аргумент в `RANK`/`DENSE_RANK` является нестандартным расширением YQL.

{% endnote %}

#### Сигнатура
```
RANK([T])->Uint64
DENSE_RANK([T])->Uint64
```

#### Примеры
``` yql
SELECT
   RANK(my_column) OVER w
FROM my_table
WINDOW w AS (ORDER BY key);
```
``` yql
SELECT
   RANK() OVER w
FROM my_table
WINDOW w AS (ORDER BY my_column);


## SessionState() {#session-state}

Нестандартная оконная функция `SessionState()` (без аргументов) позволяет получить состояние расчета сессий из [SessionWindow](../syntax/group_by.md#session-window) для текущей строки.
Допускается только при наличии `SessionWindow()` в секции `PARTITION BY` определения окна.

