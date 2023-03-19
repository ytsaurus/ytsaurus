---
vcsPath: yql/docs_yfm/docs/ru/yql-product/differences_from/ansi.md
sourcePath: yql-product/differences_from/ansi.md
---
# Отличия SQL диалекта YQL от ANSI SQL

В данном разделе описываются различие SQL диалекта YQL и стандарта ISO/IEC 9075-2 (part 2: Foundation) в версии SQL-2011.
Сравнение делается по таблице №38 из стандарта
("Feature taxonomy and definition for mandatory features").


## Отличия в поведении YQL от ANSI SQL

В данной таблице собраны сценарии когда один и тот же запрос по-разному работает в YQL и в ANSI SQL.

| Идентификатор | Название | Отличия в поведении |
| --- | --- | --- |
| E011-04 | Arithmetic operators | В YQL подавляются ошибки в арифметических вычислениях. Например выражение `1/0` имеет тип `Int32?` и значение `NULL`. В ANSI SQL в этом случае возникает ошибка |
| E011-06 | Implicit casting among the numeric data types | В YQL нет неявного кастинга между NUMERIC и INTEGER/FLOAT. |
| E031-01 | Delimited identifiers | В YQL выражение `SELECT "a" FROM foo` возвращает строковую константу. В ANSI `"a"` – это идентификатор (колонка `a`) |
| E071-02 | UNION ALL table operator | В ANSI SQL колонки объединяются по позиционному принципу, в YQL - по именам. В YQL `ORDER BY, OFFSET, LIMIT` относится к последнему элементу объединения. В ANSI - к результату `UNION ALL`.

## Статус поддержки ANSI SQL
| Идентификатор | Название | Поддержка | Комментарий |
| --- | --- | --- | --- |
| **E011** | **Numeric data types** | **Частично** |  |
| E011-01 | INTEGER and SMALLINT data types | Да | Дополнительно имеется поддержка знаковые и беззнаковые интегральные типы: `[U]INT{8,16,32,64}` и `TINYINT`.  |
| E011-02 | REAL, DOUBLE PRECISION and FLOAT data types data types | Частично | `FLOAT(<binary_precision>)` не поддерживается |
| E011-03 | DECIMAL and NUMERIC data types | Да | Поддерживается `DECIMAL(p,s)` |
| E011-04 | Arithmetic operators | Да |  |
| E011-05 | Numeric comparison | Да |  |
| E011-06 | Implicit casting among the numeric data types | Частично | Стандарт допускает неявное преобразование всех numeric типов друг к другу. У нас неявно кастятся только "родственные" типы. |
| [**E021**](####E021 Character string types) | **Character string types** | **Частично** | Не поддерживается `CHAR(len)`, `VARCHAR(len)` |
| E021-01 | CHARACTER data type | Нет |  |
| E021-02 | CHARACTER VARYING data type | Частично | Ближайшим аналогом является `String` |
| E021-03 | Character literals | Частично | Не поддерживается автоконкатенация `'aaa' 'bbb'` |
| E021-04 | CHARACTER_LENGTH function | Нет |  |
| E021-05 | OCTET_LENGTH function| Частично | ближайшим аналогом является `LEN()` |
| E021-06 | SUBSTRING | Частично | Отличие в синтаксисе |
| E021-07 | Character concatenation | Да | Поддерживается для типа String |
| E021-08 | UPPER and LOWER functions | Нет | Доступны через String UDF |
| E021-09 | TRIM function | Нет | Может быть реализована через String UDF |
| E021-10 | Implicit casting among the fixed-length and variable-length character string types | Нет | Отсутствуют соответствующие типы |
| E021-11 | POSITION function | Частично | Аналогом является `FIND` |
| E021-12 | Character comparison | Частично | Сравнение для типа String выполняется побайтно |
| **E031** | **Identifiers** | **Частично** |  |
| E031-01 | Delimited identifiers | Частично | В YQL используется бэктики для идентификаторов вместо двойных кавычек по стандарту |
| E031-02 | Lower case identifiers | Да |  |
| E031-03 | Trailing underscore | Да |  |
| **E051** | **Basic query specification** | **Частично** |  |
| E051-01 | SELECT DISTINCT | Частично | Не поддерживается `SELECT DISTINCT *` |
| E051-02 | GROUP BY clause | Да |  |
| E051-04 | GROUP BY can contain columns not in \<select list\> | Да | |
| E051-05 | Select items can be renamed | Да | `SELECT foo AS bar` |
| E051-06 | HAVING clause | Да |  |
| E051-07 | Qualified * in select list | Да | `SELECT a.*, 1 AS foo FROM T AS a` |
| E051-08 | Correlation name in the FROM clause | Да | `SELECT ... FROM T AS a` |
| E051-09 | Rename columns in the FROM clause | Нет | `SELECT ... FROM T AS a (x AS foo, y AS bar)` |
| **E061** | **Basic predicates and search conditions** | **Частично** |  |
| E061-01 | Comparison predicate | Да | Операторы `<,>,=,<>,<=,>=` |
| E061-02 | BETWEEN predicate | Частично |  |
| E061-03 | IN predicate with list of values| Да |  |
| E061-04 | LIKE predicate| Да |  |
| E061-05 | LIKE predicate: ESCAPE clause| Да | |
| E061-06 | NULL predicate| Да | |
| E061-07 | Quantified comparison predicate| Нет | `X < ANY (...)` |
| E061-08 | EXISTS predicate | Да |  |
| E061-09 | Subqueries in comparison predicate | Да | Скалярный контекст для подзапроса |
| E061-11 | Subqueries in IN predicate | Да |  |
| E061-12 | Subqueries in quantified comparison predicate | Нет |  |
| E061-13 | Correlated subqueries | Нет |  |
| E061-14 | Search condition | Да | `SELECT ... WHERE <search condition>` |
| **E071** | **Basic query expressions** | **Частично** | Объединение результатов нескольких запросов |
| E071-01 | UNION DISTINCT table operator | Нет |  |
| E071-02 | UNION ALL table operator | Да |  |
| E071-03 | EXCEPT DISTINCT table operator | Нет |  |
| E071-05 | Columns combined via table operators need not have exactly the same data type | Да | `UNION ALL` разрешает совместимые типы для колонок|
| E071-06 | Table operators in subqueries | Да | `UNION ALL` может использоваться в подзапросах |
| **E081** | **Basic privileges** | **Нет** | Привилегии для INSERT/SELECT/DELETE/UPDATE |
| **E091** | **Set functions** | **Да** |  |
| E091-01 | AVG | Да |  |
| E091-02 | COUNT | Да |  |
| E091-03 | MAX | Да |  |
| E091-04 | MIN | Да |  |
| E091-05 | SUM | Да |  |
| E091-06 | ALL quantifier | Да | `SELECT AVG(ALL x)` |
| E091-07 | DISTINCT quantifier | Да | `SELECT AVG(DISTINCT x)` |
| **E101** | **Basic data manipulation** | **Да** |  |
| E101-01 | INSERT statement | Да |  |
| E101-03 | Searched UPDATE statement | Да |  |
| E101-04 | Searched DELETE statement | Да |  |
| **E111** | **Single row SELECT statement** | **Нет** | `SELECT ... INTO <param1>, <param2> ... FROM` |
| **E121** | **Basic cursor support** | **Нет** | `DECLARE CURSOR` |
| **E131** | **Null value support (nulls in lieu of values)** | **Да** |  |
| **E141** | **Basic integrity constraints** | **Частично** |  |
| E141-01 | NOT NULL constraints | Нет |  |
| E141-02 | UNIQUE constraint of NOT NULL columns | Нет |  |
| E141-03 | PRIMARY KEY constraints | Да |  |
| E141-04 | Basic FOREIGN KEY constraint with the NO ACTION default for both referential delete action and referential update action | ? |  |
| E141-06 | CHECK constraint | Нет |  |
| E141-07 | Column defaults | Нет |  |
| E141-08 | NOT NULL inferred on PRIMARY KEY | Да |  |
| E141-10 | Names in a foreign key can be specified in any order | ? |  |
| **E151** | **Transaction support** | **Нет** | `COMMIT` в YQL упорядочивает операции в пределах одной транзакции |
| E151-01 | COMMIT statement | Нет |  |
| E151-02 | ROLLBACK statement | Нет |  |
| **E152** | **Basic SET TRANSACTION statement** | **Нет** | `ISOLATION LEVEL` etc |
| E152-01 | SET TRANSACTION statement: ISOLATION LEVEL SERIALIZABLE clause | Нет |  |
| E152-02 | SET TRANSACTION statement: READ ONLY and READ WRITE clauses | Нет |  |
| **E153** | **Updatable queries with subqueries** | **Нет** | ? |
| **E161** | **SQL comments using leading double minus** | **Да** |  |
| **E171** | **SQLSTATE support** | **Нет** |  |
| **E182** | **Host language binding** | **Нет** |  |


#### E021 Character string types

Должны поддерживаться:
* `CHARACTER[(len)]` (он же `CHAR`)- строковой тип фиксированной длинны
* `VAR CHAR[(len)]` (он же `CHARACTER VARYING`) - строковой тип переменной длинны

Должен так же поддерживаться опциональный спецификатор `CHARACTER SET cs` и возможность задать правила сортировки с помощью `COLLATE`

В YQL эти типы не поддерживаются. Ближайшим аналогом является тип `String`,
который представляет собой бинарную строку переменной длинны.

##### E021-03 Character literals
По стандарту, строковой литерал имеет следующий вид
```
<character string literal> ::=
  [ <introducer> <character set specification> ]
      <quote> [ <character representation>... ] <quote>
      [ { <separator> <quote> [ <character representation>... ] <quote> }... ]
<separator> ::=
  { <comment> | <white space> }...
<introducer> ::=
  <underscore>
<character representation> ::=
    <nonquote character>
  | <quote symbol>
<quote symbol> ::=
  <quote> <quote>
```

В YQL:
* Не поддерживается экранирование через `'`
* Не поддерживается `<character set specification>`
* Не поддерживается неявная конкатенация литералов (`'aaa' 'bbb'` по стандарту означает то же что и `'aaabbb`)

С другой стороны:
* Строковые литералы можно заключать в двойные кавычки `"aaa"`
* Работает C-style экранирование через бэкслеш
* Поддерживаются многострочные строковые литералы

##### E021-05 OCTET_LENGTH function
```
CHARACTER_LENGTH(<character value expression>) [USING CHARACTERS | OCTETS]
OCTET_LENGTH(<string value expression>)
```
В YQL поддерживается `LENGTH(<string value expressiom>)` которая возвращает длину строки в байтах

##### E021-06 SUBSTRING function
```
<character substring function> ::=
  SUBSTRING <left paren> <character value expression> FROM <start position>
      [ FOR <string length> ] [ USING CHARACTERS | OCTETS ] <right paren>
```
В YQL поддерживается в варианте `SUBSTRING(str, start, len)`.
Нумерация символов в строке в YQL начинается с нуля

##### E021-09 TRIM function
```
 TRIM([[LEADING | TRAILING | BOTH] [<char value expression>] FROM] <char value expression>)
```
Удаления префикса/суффикса из строки. В YQL не поддерживается, функциональный эквивалент можно получить с использованием String/Unicode UDF

##### E021-11 POSITION function
```
POSITION(S1 IN S2) [USING CHARACTERS | OCTETS]
```
Поиск подстроки S1 в S2.

В YQL доступен в варианте `FIND(S2, S1)`. Позиции нумеруются с 0, если строка не найдена, возвращается NULL.

#### E031 Identifiers
По стандарту, идентификаторы (например имена таблиц и колонок) могут являться
regular identifiers и delimited identifiers.
Regular identifiers начинаются с буквы и могут содержать только буквы, цифры и символы подчеркивания.


##### E031-01 Delimited identifiers
Delimited identifiers заключены в кавычки: `"abc def"` и могут содержать произвольные символы.
Кавычки экранируются двойными кавычками: `"complex ""identifier"""`

В YQL вместо кавычек используются бэктики

#### E051 Basic query specification
Общий вид SELECT запроса в стандарте такой
```
<query specification> ::=
  SELECT [ <set quantifier> ] <select list> <table expression>
<set quantifier> ::=
    DISTINCT
  | ALL
<select list> ::=
    <asterisk>
  | <select sublist> [ { <comma> <select sublist> }... ]
<select sublist> ::=
    <derived column>
  | <qualified asterisk>
<qualified asterisk> ::=
    <asterisked identifier chain> <period> <asterisk>
  | <all fields reference>
<asterisked identifier chain> ::=
  <asterisked identifier> [ { <period> <asterisked identifier> }... ]
<asterisked identifier> ::=
  <identifier>
<derived column> ::=
  <value expression> [ <as clause> ]
<as clause> ::=
  [ AS ] <column name>
<all fields reference> ::=
  <value expression primary> <period> <asterisk>
      [ AS <left paren> <all fields column name list> <right paren> ]
<all fields column name list> ::=
  <column name list>

<table expression> ::=
  <from clause>
      [ <where clause> ]
      [ <group by clause> ]
      [ <having clause> ]
      [ <window clause> ]
<from clause> ::=
  FROM <table reference list>
<table reference list> ::=
  <table reference> [ { <comma> <table reference> }... ]

```

В YQL не поддерживаются множественные таблицы после `FROM` (`<table reference list>` содержит более одного элемента)

##### E051-01 SELECT DISTINCT

SELECT DISTINCT поддерживается за исключением варианта с asterisk/qualified asterisk

##### E051-07 Qualified * in select list

В YQL:
* `<asterisk identifier chain>` может состоять только из одного элемента
* В `<all fields reference>` не поддерживаются алиасы `<all fields column name list>`
* `<value expression primary>` может содержать только ссылку на таблицу


##### E061-01 Comparison predicate

В YQL поддерживаются операторы <,>,=,<>,<=,>= для совместимых типов

##### E061-02 BETWEEN predicate

```
<between predicate> ::=
  <row value predicand> <between predicate part 2>
<between predicate part 2> ::=
  [ NOT ] BETWEEN [ ASYMMETRIC | SYMMETRIC ]
      <row value predicand> AND <row value predicand>
```

В YQL не поддерживаются SYMMETRIC/ASSYMETRIC - `A BETWEEN B AND C` эквивалентно `A >= B AND A <= C`.

##### E061-07 Quantified comparison predicate

```
<quantified comparison predicate> ::=
  <row value predicand> <quantified comparison predicate part 2>
<quantified comparison predicate part 2> ::=
  <comp op> <quantifier> <table subquery>
<quantifier> ::=
    <all>
  | <some>
<all> ::= ALL
<some> ::=
    SOME
  | ANY
```
