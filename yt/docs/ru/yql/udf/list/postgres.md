# PostgreSQL UDF

YQL предоставляет возможность доступа из YQL к [функциям](https://www.postgresql.org/docs/16/functions.html) и [типам данных](https://www.postgresql.org/docs/16/datatype.html) PostgreSQL.

Также вы можете написать весь запрос в синтаксисе PostgreSQL.

Ключевые особенности текущей реализации, которые стоит иметь в виду:

{% if audience == "internal" %}
1. Используется PostgreSQL из [аркадии](https://a.yandex-team.ru/arc_vcs/contrib/libs/postgresql).
{% endif %}
1. В данный момент поддерживаются обычные функции и агрегационные, в том числе при агрегации на окне.
1. Нативные типы PostgreSQL можно непосредственно сохранять в таблицах {{product-name}} (без конвертации в YQL типы).
1. {{product-name}} не поддерживает правильной сортировки значений PostgreSQL типов за исключением строк, чисел и булевых значений.

## Типы данных

Стоит заметить, что система типов в PostgreSQL существенно проще, чем в YQL:
1. Все типы в PostgreSQL являются nullable (аналогом типа int4 в postgres является yql тип Int32?).
2. Единственно возможный сложный (контейнерный) тип в PostgreSQL – это массив (array) с некоторой размерностью. Вложенные массивы невозможны на уровне типов.

На данный момент поддерживаются все простые типы данных из PostgreSQL, включая массивы.

Имена PostgreSQL типов в YQL получаются добавлением префикса `Pg` к исходному имени типа.
Например `PgVarchar`, `PgInt4`, `PgText`. Имена pg типов (как и вообще всех типов) в YQL являются case-insensitive.

Если исходный тип является типом массива (в PostgreSQL такие типы начинаются с подчеркивания: `_int4` - массив 32-битных целых), то имя типа в YQL тоже начинается с подчеркивания – `_PgInt4`.

## Литералы {#literals}

Строковые и числовые литералы Pg типов можно создавать с помощью специальных суффиксов (аналогично простым [строковым](../../syntax/lexer.md#string-literals) и [числовым](../../syntax/lexer.md#literal-numbers) литералам).

### Целочисленные литералы {#intliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgInt4` | 32-битное знаковое целое (в PostgreSQL нет беззнаковых типов)
`ps`| `PgInt2` | 16-битное знаковое целое
`pi`| `PgInt4` |
`pb`| `PgInt8` | 64-битное знаковое цело
`pn`| `PgNumeric` | знаковое целое произвольной точности (до 131072 цифр)

### Литералы с плавающей точкой {#floatliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgFloat8` | число с плавающей точкой (64 бит double)
`pf4`| `PgFloat4` | число с плавающей точкой (32 бит float)
`pf8`| `PgFloat8` |
`pn` | `PgNumeric` | число с плавающей точкой произвольной точности (до 131072 цифр перед запятой, до 16383 цифр после запятой)

### Строковые литералы {#stringliterals}

Суффикс | Тип | Комментарий
----- | ----- | -----
`p` | `PgText` | текстовая строка
`pt`| `PgText` |
`pv`| `PgVarchar` | текстовая строка
`pb`| `PgBytea` | бинарная строка

{% note warning "Внимание" %}

Значения строковых/числовых литералов (т.е. то что идет перед суффиксом) должны быть валидной строкой/числом с точки зрения YQL.
В частности, должны соблюдаться правила эскейпинга YQL, а не PostgreSQL.

{% endnote %}

Пример:

```yql
SELECT
    1234p,       -- pgint4
    0x123pb,     -- pgint8
    "тест"pt,    -- pgtext
    123e-1000pn; -- pgnumeric
;
```

### Литерал массива {#array-literal}

Для построения литерала массива используется функция `PgArray`:

```yql
SELECT
    PgArray(1p, NULL ,2p) -- {1,NULL,2}, тип _int4
;
```

### Конструктор литералов произвольного типа {#literals_constructor}

Литералы всех типов (в том числе и Pg типов) могут создаваться с помощью конструктора литералов со следующей сигнатурой:
`Имя_типа(<строковая константа>)`.

Напрмер:

```yql
DECLARE $foo AS String;
SELECT
    PgInt4("1234"), -- то же что и 1234p
    PgInt4(1234),   -- в качестве аргумента можно использовать литеральные константы
    PgInt4($foo),   -- и declare параметры
    PgBool(true),
    PgInt8(1234),
    PgDate("1932-01-07"),
;
```

Также поддерживается встроенная функция `PgConst` со следующей сигнатурой: `PgConst(<строковое значение>, <тип>)`.
Такой способ более удобен для кодогенерации.

Например:

```yql
SELECT
    PgConst("1234", PgInt4), -- то же что и 1234p
    PgConst("true", PgBool)
;
```

Для некоторых типов в функции `PgConst` можно указать дополнительные модификаторы. Возможные модификаторы для типа `pginterval` перечислены в [документации PostgreSQL](https://www.postgresql.org/docs/16/datatype-datetime.html).

```yql
SELECT
    PgConst(90, pginterval, "day"), -- 90 days
    PgConst(13.45, pgnumeric, 10, 1); -- 13.5
;
```


## Операторы {#operators}

Операторы PostgreSQL (унарные и бинарные) доступны через встроенную функцию `PgOp(<оператор>, <операнды>)`:

```yql
SELECT
    PgOp("*", 123456789987654321pn, 99999999999999999999pn), --  12345678998765432099876543210012345679
    PgOp('|/', 10000.0p), -- 100.0p (квадратный корень)
    PgOp("-", 1p), -- -1p
    -1p,           -- унарный минус для литералов работает и без PgOp
;
```

## Оператор приведения типа {#cast_operator}

Для приведения значения одного Pg типа к другому используется встроенная функция `PgCast(<исходное значение>, <желаемый тип>)`:

```yql
SELECT
    PgCast(123p, PgText), -- преобразуем число в строку
;
```

При преобразовании из строковых Pg типов в некоторые целевые типы можно указать дополнительные модификаторы. Возможные модификаторы для типа `pginterval` перечислены в [документации](https://www.postgresql.org/docs/16/datatype-datetime.html).

```yql
SELECT
    PgCast('90'p, pginterval, "day"), -- 90 days
    PgCast('13.45'p, pgnumeric, 10, 1); -- 13.5
;
```

## Преобразование значений Pg типов в значения YQL типов и обратно {#frompgtopg}

Для некоторых Pg типов возможна конвертация в YQL типы и обратно. Конвертация осуществляется с помощью встроенных функций
`FromPg(<значение Pg типа>)` и `ToPg(<значение YQL типа>)`:

```yql
SELECT
    FromPg("тест"pt), -- Just(Utf8("тест")) - pg типы всегда nullable
    ToPg(123.45), -- 123.45pf8
;
```

### Список псевдонимов типов PostgreSQL при их использовании в YQL {#pgyqltypes}

Ниже приведены типы данных YQL, соответствующие им логические типы PostgreSQL и названия типов PostgreSQL при их использовании в YQL:

| YQL | PostgreSQL | Название PostgreSQL-типа в YQL|
|---|---|---|
| `Bool` | `bool` |`pgbool` |
| `Int8` | `int2` |`pgint2` |
| `Uint8` | `int2` |`pgint2` |
| `Int16` | `int2` |`pgint2` |
| `Uint16` | `int4` |`pgint4` |
| `Int32` | `int4` |`pgint4` |
| `Uint32` | `int8` |`pgint8` |
| `Int64` | `int8` |`pgint8` |
| `Uint64` | `numeric` |`pgnumeric` |
| `Float` | `float4` |`pgfloat4` |
| `Double` | `float8` |`pgfloat8` |
| `String` | `bytea` |`pgbytea` |
| `Utf8` | `text` |`pgtext` |
| `Yson` | `bytea` |`pgbytea` |
| `Json` | `json` |`pgjson` |
| `Uuid` | `uuid` |`pguuid` |
| `JsonDocument` | `jsonb` |`pgjsonb` |
| `Date` | `date` |`pgdate` |
| `Datetime` | `timestamp` |`pgtimestamp` |
| `Timestamp` | `timestamp` |`pgtimestamp` |
| `Interval` | `interval` | `pginterval` |
| `TzDate` | `text` |`pgtext` |
| `TzDatetime` | `text` |`pgtext` |
| `TzTimestamp` | `text` |`pgtext` |
| `Date32` | `date` | `pgdate`|
| `Datetime64` | `timestamp` |`pgtimestamp` |
| `Timestamp64` | `timestamp` |`pgtimestamp` |
| `Interval64`| `interval` |`pginterval` |
| `TzDate32` | `text` |`pgtext` |
| `TzDatetime64` | `text` |`pgtext` |
| `TzTimestamp64` | `text` |`pgtext` |
| `Decimal` | `numeric` |`pgnumeric` |
| `DyNumber` | `numeric` |`pgnumeric` |


### Таблица соответствия типов `ToPg` {#topg}

Таблица соответствия типов данных YQL и PostgreSQL при использовании функции `ToPg`:

| YQL | PostgreSQL | Название PostgreSQL-типа в YQL |
|---|---|---|
| `Bool` | `bool` |`pgbool` |
| `Int8` | `int2` |`pgint2` |
| `Uint8` | `int2` |`pgint2` |
| `Int16` | `int2` |`pgint2` |
| `Uint16` | `int4` |`pgint4` |
| `Int32` | `int4` |`pgint4` |
| `Uint32` | `int8` |`pgint8` |
| `Int64` | `int8` |`pgint8` |
| `Uint64` | `numeric` |`pgnumeric` |
| `Float` | `float4` |`pgfloat4` |
| `Double` | `float8` |`pgfloat8` |
| `String` | `bytea` |`pgbytea` |
| `Utf8` | `text` |`pgtext` |
| `Yson` | `bytea` |`pgbytea` |
| `Json` | `json` |`pgjson` |
| `Uuid` | `uuid` |`pguuid` |
| `JsonDocument` | `jsonb` |`pgjsonb` |
| `Date` | `date` |`pgdate` |
| `Datetime` | `timestamp` |`pgtimestamp` |
| `Timestamp` | `timestamp` |`pgtimestamp` |
| `Interval` | `interval` | `pginterval` |
| `TzDate` | `text` |`pgtext` |
| `TzDatetime` | `text` |`pgtext` |
| `TzTimestamp` | `text` |`pgtext` |
| `Date32` | `date` | `pgdate`|
| `Datetime64` | `timestamp` |`pgtimestamp` |
| `Timestamp64` | `timestamp` |`pgtimestamp` |
| `Interval64`| `interval` |`pginterval` |
| `TzDate32` | `text` |`pgtext` |
| `TzDatetime64` | `text` |`pgtext` |
| `TzTimestamp64` | `text` |`pgtext` |
| `Decimal` | `numeric` |`pgnumeric` |
| `DyNumber` | `numeric` |`pgnumeric` |

### Таблица соответствия типов `FromPg` {#frompg}

Таблица соответствия типов данных PostgreSQL и YQL при использовании функции `FromPg`:

| PostgreSQL | YQL |
|---|---|
| `bool` | `bool` |
| `int2` | `Int16` |
| `int4` | `Int32` |
| `int8` | `Int64` |
| `float4` | `Float` |
| `float8` | `Double` |
| `bytea` | `String` |
| `varchar` | `Utf8` |
| `text` | `Utf8` |
| `cstring` | `Utf8` |
| `uuid` | `Uuid` |
| `date` | `Date32` |
| `timestamp` | `Timestamp64` |

## Вызов PostgreSQL функций {#callpgfunction}

Чтобы вызвать PostgreSQL функцию, необходимо добавить префикс `Pg::` к ее имени:

```yql
SELECT
    Pg::extract('isodow'p,PgCast('1961-04-12'p,PgDate)), -- 3pn (среда) - работа с датами до 1970 года
    Pg::generate_series(1p,5p), -- [1p,2p,3p,4p,5p] - для функций-генераторов возвращается ленивый список
;
```

Существует также альтернативный способ вызова функций через встроенную функцию `PgCall(<имя функции>, <операнды>)`:

```yql
SELECT
    PgCall('lower', 'Test'p), -- 'test'p
;
```

При вызове функции, возвращающей набор `pgrecord`, можно распаковать результат в список структур, используя функцию `PgRangeCall(<имя функции>, <операнды>)`:

```yql
SELECT * FROM
    AS_TABLE(PgRangeCall("json_each", pgjson('{"a":"foo", "b":"bar"}')));
    --- 'a'p,pgjson('"foo"')
    --- 'b'p,pgjson('"bar"')
;
```

## Вызов агрегационных PostgreSQL функций {#pgaggrfunction}

Чтобы вызвать агрегационную PostgreSQL функцию, необходимо добавить префикс `Pg::` к ее имени:

```yql
SELECT
Pg::string_agg(x,','p)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
Pg::string_agg(x,','p) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
;
```

Также можно использовать агрегационную PostgreSQL функцию для построения фабрики агрегационных функций с последующим применением в `AGGREGATE_BY`:

```yql
$agg_max = AggregationFactory("Pg::max");

SELECT
AGGREGATE_BY(x,$agg_max)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'c'p

SELECT
AGGREGATE_BY(x,$agg_max) OVER (ORDER BY x),
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'b'p,'c'p
```

В этом случае вызов `AggregationFactory` принимает только имя функции с префиксом `Pg::`, а все аргументы функции передаются в `AGGREGATE_BY`.

Если в агрегационной функции не один аргумент, а ноль или два и более, необходимо использовать кортеж при вызове `AGGREGATE_BY`:

```yql
$agg_string_agg = AggregationFactory("Pg::string_agg");

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a,b,c'p

SELECT
AGGREGATE_BY((x,','p),$agg_string_agg) OVER (ORDER BY x)
FROM (VALUES ('a'p),('b'p),('c'p)) as a(x); -- 'a'p,'a,b'p,'a,b,c'p
```

{% note warning "Внимание" %}

Не поддерживается режим `DISTINCT` над аргументами при вызове агрегационных PostgreSQL функций, а также использование `MULTI_AGGREGATE_BY`.

{% endnote %}

## Логические операции {#logic-operations}

Для выполнения логических операций используются функции `PgAnd`, `PgOr`, `PgNot`:

```yql
SELECT
    PgAnd(PgBool(true), PgBool(true)), -- PgBool(true)
    PgOr(PgBool(false), null), -- PgCast(null, pgbool)
    PgNot(PgBool(true)), -- PgBool(false)
;
```

<!--
Not supported yet.

## Использование PostgreSQL синтаксиса

После выбора специального синтаксиса запроса, вы можете писать запросы на обычном PostgreSQL диалекте, при этом типы PostgreSQL не требуют префикса `pg`.

Пример:
``` sql
SELECT
    upper('foo' || 1::text)
```

При этом доступны большинство встроенных функций PostgreSQL.

Не все синтаксические конструкции PostgreSQL в настоящий момент поддерживаются, но их поддержка достаточно полная для того чтобы выполнялись тесты TPC-H и TPC-DS, в частности есть поддержка кореллирующих подзапросов.

### Чтение из исходных таблиц

Для доступа к таблицам необходимо указывать имя кластера через точку:

``` sql
SELECT
    region
FROM
    hahn."//home/yql/tutorial/users"
```

При чтении из исходных таблиц, если колонки в них не являются PostgreSQL типами, поддерживаются неявные преобразования (включая `Optional` над ними):

* `Bool` -> `pgboolean`
* `Int8` -> `pgint2`
* `Uint8` -> `pgint2`
* `Int16` -> `pgint2`
* `Uint16` -> `pgint4`
* `Int32` -> `pgint4`
* `Uint32` -> `pgint8`
* `Int64` -> `pgint8`
* `Uint64` -> `pgnumeric`
* `Float` -> `pgfloat4`
* `Double` -> `pgfloat8`
* `Decimal` -> `pgnumeric`
* `DyNumber` -> `pgnumeric`
* `String` -> `pgbytea`
* `Yson` -> `pgbytea`
* `Utf8` -> `pgtext`
* `Json` -> `pgjson`
* `JsonDocument` -> `pgjsonb`
* `Uuid` -> `pguuid`
* `Date` -> `pgdate`
* `Datetime` -> `pgtimestamp`
* `Timestamp` -> `pgtimestamp`
* `TzDate` -> `pgtext`
* `TzDatetime` -> `pgtext`
* `TzTimestamp` -> `pgtext`
* `Interval` -> `pginterval`

Все остальные типы исходных колонок не поддерживаются, например `List` или вложенные `Optional`.

### Чтение нескольких входных таблиц

После имени кластера можно вызвать одну из табличных функций, которая объединяет найденные таблицы в одну по принципу `UNION ALL`. Все аргументы этих функций должны быть строковыми литералами.

Функции:

* `concat('table1'[,'table2'...])` - объединить явно перечисленные таблицы
* `concat_view('table1','view1'[,'table2','view2',...])` - объединить явно перечисленные таблицы с учетом `VIEW` в них
* `range('dir'[,'from','to','suffix','view'])` - получить список таблиц в папке `dir` с именем от `from` до `to`
* `like('dir','pattern'[,'suffix','view'])` - получить список таблиц в папке `dir` имя которых подходит под шаблон `LIKE`
* `regexp('dir','pattern'[,'suffix','view'])` - получить список таблиц папке `dir` имя которых подходит под шаблон регулярного выражения

Пример:
``` sql
SELECT
    count(*)
FROM
    hahn.range('//home/yql/tutorial','users','users')
```

### Запись выходных таблиц

Для доступа к таблицам необходимо указывать имя кластера через точку.

При записи таблиц в PostgreSQL синтаксисе выходные колонки будут созданы с PostgreSQL типами.

При записи выходных таблиц возможна только дозапись таблиц или запись в несуществующую таблицу, режим `TRUNCATE` не поддерживается:

``` sql
INSERT INTO <cluster-name>."//tmp/test1"
SELECT
    region
FROM
    <cluster-name>."//home/yql/tutorial/users"
```

### Повторное использование подзапросов (VIEW/CTE)

Есть поддержка эфемерных `VIEW` в запросе, которые должны быть удалены в конце запроса:

``` sql
CREATE VIEW Regions(region) AS (
    SELECT
        region
    FROM
        <cluster-name>."//home/yql/tutorial/users"
);

SELECT * FROM Regions;

DROP VIEW Regions;
```

Если подзапрос используется только в одном `SELECT` верхнего уровня, его можно выразить через `CTE`:

``` sql
WITH Regions(region) AS (
    SELECT
        region
    FROM
        <cluster-name>."//home/yql/tutorial/users"
)
SELECT * FROM Regions;
```

### PRAGMA

Поддерживается задание `PRAGMA` в следующем виде (возможные префиксы - `yt.`, `dq.`):

``` sql
set DqEngine='auto';
set yt.Pool = 'yql_perf';
```
-->