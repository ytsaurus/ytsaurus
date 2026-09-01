# OVER, PARTITION BY и WINDOW

Механизм оконных функций, появившийся в стандарте SQL:2003 и расширенный в стандарте SQL:2011, позволяет выполнять вычисления над набором строк таблицы, который некоторым образом соотносится с текущей строкой.

В отличие от [агрегатных функций](../builtins/aggregation.md) при этом не происходит группировка нескольких строк в одну – после применения оконных функций число строк в результирующей таблице всегда совпадает с числом строк в исходной.

При наличии в запросе агрегатных и оконных функций сначала производится группировка и вычисляются значения агрегатных функций. Вычисленные значения агрегатных функций могут использоваться в качестве аргументов оконных (но не наоборот). Порядок, в котором вычисляются оконные функции относительно других элементов запроса, описан в разделе [SELECT](select/index.md).

## Синтаксис {#syntax}

Общий синтаксис вызова оконной функции имеет вид

```yql
function_name([expression [, expression ...]]) OVER (window_definition)
```

или

```yql
function_name([expression [, expression ...]]) OVER window_name
```

Здесь `window_name` (*имя окна*) – произвольный идентификатор, уникальный в рамках запроса, `expression` – произвольное выражение не содержащее вызова оконных функций.

В запросе каждому имени окна должно быть сопоставлено *определение окна* (`window_definition`):

```yql
SELECT
    F0(...) OVER (window_definition_0),
    F1(...) OVER w1,
    F2(...) OVER w2,
    ...
FROM my_table
WINDOW
    w1 AS (window_definition_1),
    ...
    w2 AS (window_definition_2)
;
```

Здесь `window_definition` записывается в виде

```antlr
[ PARTITION BY (expression AS column_identifier | column_identifier) [, ...] ]
[ ORDER BY expression [ASC | DESC] ]
[ frame_definition ]
```

Необязательное *определение рамки* (`frame_definition`) может быть задано одним из следующих способов:

* ```ROWS frame_begin```
* ```ROWS BETWEEN frame_begin AND frame_end```
* ```RANGE frame_begin```
* ```RANGE BETWEEN frame_begin AND frame_end```

{% note info %}

Режим `RANGE` доступен начиная с версии языка 2026.01.

В Query Tracker для использования рамок `RANGE` выберите версию языка 2026.01 или новее.

{% endnote %}

*Начало рамки* (`frame_begin`) и *конец рамки* (`frame_end`) задаются одним из следующих способов:

* ```UNBOUNDED PRECEDING```
* ```offset PRECEDING```
* ```CURRENT ROW```
* ```offset FOLLOWING```
* ```UNBOUNDED FOLLOWING```

Здесь *смещение рамки* (`offset`) — неотрицательный литерал. Если конец рамки не задан, подразумевается `CURRENT ROW`.

В режиме `ROWS` смещение всегда целочисленное. В режиме `RANGE` со смещением `ORDER BY` должен содержать ровно один столбец; тип смещения определяется типом этого столбца и должен поддерживать с ним сложение, вычитание и сравнение. Для числовых типов, включая `Decimal`, используется числовое смещение; для типов даты и времени — `Interval` или `Interval64`. PostgreSQL-типы поддерживаются аналогично. Значения `NaN` и `Inf` использовать нельзя.

Все выражения внутри определения окна не должны содержать вызовов оконных функций.

## Алгоритм вычисления

### Разбиение {#partition}

Указание `PARTITION BY` группирует строки исходной таблицы в *разделы*, которые затем обрабатываются независимо друг от друга. Если `PARTITION BY` не указан, то все строки исходной таблицы попадают в один раздел. Указание `ORDER BY` определяет порядок строк в разделе.

В `PARTITION BY`, как и в [GROUP BY](group_by.md) можно использовать алиасы и [SessionWindow](group_by.md#session-window).

При отсутствии `ORDER BY` порядок строк в разделе не определён.

### Рамка {#frame}

Определение рамки `frame_definition` задаёт множество строк раздела, попадающих в *рамку окна*, связанную с текущей строкой.

В режиме `ROWS` в рамку окна попадают строки с указанными смещениями относительно текущей строки раздела. Например, для `ROWS BETWEEN 3 PRECEDING AND 5 FOLLOWING` в рамку окна попадут три строки перед текущей, текущая строка и пять строк после неё.

В режиме `RANGE` со смещением диапазон задаётся значениями единственного столбца `ORDER BY`, а не числом строк. Например, `RANGE BETWEEN 3 PRECEDING AND 5 FOLLOWING` включает строки со значением от «текущее минус 3» до «текущее плюс 5». `RANGE CURRENT ROW` включает все строки с тем же значением сортировки. `UNBOUNDED PRECEDING` и `UNBOUNDED FOLLOWING` имеют тот же смысл, что в режиме `ROWS`.

Множество строк в рамке окна может меняться в зависимости от того, какая строка является текущей. Например, для первой строки раздела в рамку окна `ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING` не попадет ни одной строки.

Указание `UNBOUNDED PRECEDING` в качестве начала рамки означает "от первой строки раздела", `UNBOUNDED FOLLOWING` в качестве конца рамки – "до последней строки раздела", `CURRENT ROW` – "от/до текущей строки".

Если `определение_рамки` не указано, то в множество строк попадающих в рамку окна определяется наличием `ORDER BY` в `определении_окна`.
А именно, при наличии `ORDER BY` неявно подразумевается `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, а при отсутствии – `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`.

Далее, в зависимости от конкретной оконной функции производится ее вычисление либо на множестве строк раздела, либо на множестве строк рамки окна.

[Список доступных оконных функций](../builtins/window.md)

```yql
SELECT
    ts,
    AVG(value) OVER w AS moving_avg
FROM my_table
WINDOW w AS (
    ORDER BY ts
    RANGE BETWEEN 10 PRECEDING AND 10 FOLLOWING
);
```

#### Примеры

```yql
SELECT
    COUNT(*) OVER w AS rows_count_in_window,
    some_other_value -- доступ к текущей строке
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
);
```

```yql
SELECT
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM `my_table`
WINDOW w AS (
    PARTITION BY partition_key_column
);
```

```yql
SELECT
    -- AVG (как и все агрегатные функции, используемые в качестве оконных)
    -- вычисляется на рамке окна
    AVG(some_value) OVER w AS avg_of_prev_current_next,
    some_other_value -- доступ к текущей строке
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY int_column
    ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING
);
```

```yql
SELECT
    -- LAG не зависит от положения рамки окна
    LAG(my_column, 2) OVER w AS row_before_previous_one
FROM my_table
WINDOW w AS (
    PARTITION BY partition_key_column
    ORDER BY my_column
);
```

## Особенности реализации

* Функции на рамке `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` либо `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` вычисляются за O(размер раздела) без дополнительной памяти. `RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW` требует памяти для строк с одинаковым значением `ORDER BY`.

* Для рамки окна `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING` можно выбрать стратегию выполнения в памяти, указав [хинт](lexer.md#sql-hints) `COMPACT` после `PARTITION`.

  Например: `PARTITION /*+ COMPACT() */ BY key` или `PARTITION /*+ COMPACT() */ BY ()` (в случае если `PARTITION BY` изначально отсутствовал).

  При наличии хинта `COMPACT` потребуется дополнительная память в размере O(размер раздела), но при этом не возникнет дополнительной `JOIN` операции.

* Если рамка окна не начинается с `UNBOUNDED PRECEDING`, то для вычисления оконных функций на таком окне потребуется дополнительная память в размере O(максимальное расстояние от границ окна до текущей строки), а время вычисления будет равно O(число_строк_в_разделе * размер_окна).

* Для рамки, начинающейся с `UNBOUNDED PRECEDING` и заканчивающейся на `N`, где `N` не равен `CURRENT ROW` или `UNBOUNDED FOLLOWING`, потребуется O(N) дополнительной памяти, а время вычисления будет O(размер раздела).

* Функции `LEAD(expr, N)` и `LAG(expr, N)` всегда потребуют O(N) памяти.

Запрос с `ROWS/RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING` по возможности стоит переделать в `ROWS/RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`, поменяв порядок `ORDER BY` на обратный.

В терминах MapReduce оконные функции физически выполняются через Reduce по ключам `PARTITION BY`, что может означать длительное выполнение для разделов большого размера, а также жёсткий лимит в 200Гб на раздел для основных кластеров {{product-name}}.

{% if audience == "internal" %}
[Пример в tutorial](https://yql.yandex-team.ru/Tutorial/yt_11_Window_functions)
{% endif %}
