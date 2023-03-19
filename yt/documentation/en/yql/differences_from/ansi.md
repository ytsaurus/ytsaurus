---
vcsPath: yql/docs_yfm/docs/ru/yql-product/differences_from/ansi.md
sourcePath: yql-product/differences_from/ansi.md
---
# Distinctions of YQL SQL dialect from ANSI SQL

This section describes distinctions of YQL SQL dialect and the ISO/IEC 9075-2 standard (Part 2: Foundation) in the version SQL-2011.
To make the comparison, we took table 38 from the standard
("Feature taxonomy and definition for mandatory features").


## Distinctions in behavior between YQL and ANSI SQL

In this table, we've put together the scenarios where the same query produces different results in YQL and ANSI SQL.

| ID | Name | Behavioral distinctions |
| --- | --- | --- |
| E011-04 | Arithmetic operators | YQL suppresses errors in arithmetic calculations. For example, the expression `1/0` has the type `Int32?` and the value `NULL`. ANSI SQL will throw an error in this case: |
| E011-06 | Implicit casting among the numeric data types | YQL doesn't provide implicit casting between NUMERIC and INTEGER/FLOAT. |
| E031-01 | Delimited identifiers | In YQL, the expression `SELECT "a" FROM foo` returns a string constant. In ANSI, `"a"` is treated as the ID (the `a` column) |
| E071-02 | UNION ALL table operator | In ANSI SQL, columns are joined according to their positions; in YQL, they are joined by names. In YQL, `ORDER BY, OFFSET, LIMIT` apply to the last united item. In ANSI, it's applied to the `UNION ALL` result. |

## ANSI SQL support status
| ID | Name | Support | Comment |
| --- | --- | --- | --- |
| **E011** | **Numeric data types** | **Partial** |  |
| E011-01 | INTEGER and SMALLINT data types | Yes | Signed and unsigned integral types are additionally supported: `[U]INT{8,16,32,64}` and `TINYINT`. |
| E011-02 | REAL, DOUBLE PRECISION and FLOAT data types data types | Partial | `FLOAT(<binary_precision>)`isn't supported |
| E011-03 | DECIMAL and NUMERIC data types | Yes | `DECIMAL(p,s)` is supported |
| E011-04 | Arithmetic operators | Yes |  |
| E011-05 | Numeric comparison | Yes |  |
| E011-06 | Implicit casting among the numeric data types | Partial | The standard allows for implicit mutual casting between all the numeric types. We only implicitly cast "related" types. |
| [**E021**](####E021 Character string types) | **Character string types** | **Partial** | `CHAR(len)`, `VARCHAR(len)` is not supported |
| E021-01 | CHARACTER data type | No |  |
| E021-02 | CHARACTER VARYING data type | Partial | The closest analog is `String` |
| E021-03 | Character literals | Partial | Automatic `'aaa' 'bbb'` concatenation isn't supported |
| E021-04 | CHARACTER_LENGTH function | No |  |
| E021-05 | OCTET_LENGTH function | Partial | The closest analog is `LEN()` |
| E021-06 | SUBSTRING | Partial | Distinctions in syntax |
| E021-07 | Character concatenation | Yes | Supported for the String type |
| E021-08 | UPPER and LOWER functions | No | Available using String UDF |
| E021-09 | TRIM function | No | Can be implemented using String UDF |
| E021-10 | Implicit casting among the fixed-length and variable-length character string types | No | Relevant types are missing |
| E021-11 | POSITION function | Partial | The counterpart is `FIND` |
| E021-12 | Character comparison | Partial | String type is compared bitwise |
| **E031** | **Identifiers** | **Partial** |  |
| E031-01 | Delimited identifiers | Partial | YQL uses backticks for IDs rather than double quotes prescribed by the standard |
| E031-02 | Lower case identifiers | Yes |  |
| E031-03 | Trailing underscore | Yes |  |
| **E051** | **Basic query specification** | **Partial** |  |
| E051-01 | SELECT DISTINCT | Partial | `SELECT DISTINCT *` isn't supported |
| E051-02 | GROUP BY clause | Yes |  |
| E051-04 | GROUP BY can contain columns not in \<select list\> | Yes | |
| E051-05 | Select items can be renamed | Yes | `SELECT foo AS bar` |
| E051-06 | HAVING clause | Yes |  |
| E051-07 | Qualified * in select list | Yes | `SELECT a.*, 1 AS foo FROM T AS a` |
| E051-08 | Correlation name in the FROM clause | Yes | `SELECT ... FROM T AS a` |
| E051-09 | Rename columns in the FROM clause | No | `SELECT ... FROM T AS a (x AS foo, y AS bar)` |
| **E061** | **Basic predicates and search conditions** | **Partial** |  |
| E061-01 | Comparison predicate | Yes | Operators `<,>,=,<>,<=,>=` |
| E061-02 | BETWEEN predicate | Partial |  |
| E061-03 | IN predicate with list of values | Yes |  |
| E061-04 | LIKE predicate | Yes |  |
| E061-05 | LIKE predicate: ESCAPE clause | Yes | |
| E061-06 | NULL predicate | Yes | |
| E061-07 | Quantified comparison predicate | No | `X < ANY (...)` |
| E061-08 | EXISTS predicate | Yes |  |
| E061-09 | Subqueries in comparison predicate | Yes | Scalar context for sub-queries |
| E061-11 | Subqueries in IN predicate | Yes |  |
| E061-12 | Subqueries in quantified comparison predicate | No |  |
| E061-13 | Correlated subqueries | No |  |
| E061-14 | Search condition | Yes | `SELECT ... WHERE <search condition>` |
| **E071** | **Basic query expressions** | **Partial** | Joining results of multiple queries |
| E071-01 | UNION DISTINCT table operator | No |  |
| E071-02 | UNION ALL table operator | Yes |  |
| E071-03 | EXCEPT DISTINCT table operator | No |  |
| E071-05 | Columns combined via table operators need not have exactly the same data type | Yes | `UNION ALL` allows compatible types for columns |
| E071-06 | Table operators in subqueries | Yes | `UNION ALL` can be used in sub-queries |
| **E081** | **Basic privileges** | **No** | Privileges for INSERT/SELECT/DELETE/UPDATE |
| **E091** | **Set functions** | **Yes** |  |
| E091-01 | AVG | Yes |  |
| E091-02 | COUNT | Yes |  |
| E091-03 | MAX | Yes |  |
| E091-04 | MIN | Yes |  |
| E091-05 | SUM | Yes |  |
| E091-06 | ALL quantifier | Yes | `SELECT AVG(ALL x)` |
| E091-07 | DISTINCT quantifier | Yes | `SELECT AVG(DISTINCT x)` |
| **E101** | **Basic data manipulation** | **Yes** |  |
| E101-01 | INSERT statement | Yes |  |
| E101-03 | Searched UPDATE statement | Yes |  |
| E101-04 | Searched DELETE statement | Yes |  |
| **E111** | **Single row SELECT statement** | **No** | `SELECT ... INTO <param1>, <param2> ... FROM` |
| **E121** | **Basic cursor support** | **No** | `DECLARE CURSOR` |
| **E131** | **Null value support (nulls in lieu of values)** | **Yes** |  |
| **E141** | **Basic integrity constraints** | **Partial** |  |
| E141-01 | NOT NULL constraints | No |  |
| E141-02 | UNIQUE constraint of NOT NULL columns | No |  |
| E141-03 | PRIMARY KEY constraints | Yes |  |
| E141-04 | Basic FOREIGN KEY constraint with the NO ACTION default for both referential delete action and referential update action | ? |  |
| E141-06 | CHECK constraint | No |  |
| E141-07 | Column defaults | No |  |
| E141-08 | NOT NULL inferred on PRIMARY KEY | Yes |  |
| E141-10 | Names in a foreign key can be specified in any order | ? |  |
| **E151** | **Transaction support** | **No** | `COMMIT` in YQL is ordering operations within a single transaction |
| E151-01 | COMMIT statement | No |  |
| E151-02 | ROLLBACK statement | No |  |
| **E152** | **Basic SET TRANSACTION statement** | **No** | `ISOLATION LEVEL` etc |
| E152-01 | SET TRANSACTION statement: ISOLATION LEVEL SERIALIZABLE clause | No |  |
| E152-02 | SET TRANSACTION statement: READ ONLY and READ WRITE clauses | No |  |
| **E153** | **Updatable queries with subqueries** | **No** | ? |
| **E161** | **SQL comments using leading double minus** | **Yes** |  |
| **E171** | **SQLSTATE support** | **No** |  |
| **E182** | **Host language binding** | **No** |  |


#### E021 Character string types

Should be supported:
* `CHARACTER[(len)]` (aka `CHAR`): A string type of a fixed length.
* `VAR CHAR[(len)]` (aka `CHARACTER VARYING`): A string type of a variable length.

It should also support the optional specifier `CHARACTER SET cs` and setting sorting rules by `COLLATE`

These types aren't supported in YQL. The nearest analog is the `String` type
 which is a binary string of a variable length.

##### E021-03 Character literals
According to the standard, string literals have the following format:
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

In YQL:
* Escaping by `'` isn't supported
* `<Character set specification>` isn't supported
* Implicit concatenation of literals isn't supported (according to the standard, `'aaa' 'bbb'` means the same as `'aaabbb`)

On the other hand:
* You can enclose string literals into double quotes (`"aaa"`)
* C-style escaping by a backslash can also be used
* Multi-line string literals are supported

##### E021-05 OCTET_LENGTH function
```
CHARACTER_LENGTH(<character value expression>) [USING CHARACTERS | OCTETS]
OCTET_LENGTH(<string value expression>)
```
YQL suports `LENGTH(<string value expression>)` that returns the length of a string in bytes

##### E021-06 SUBSTRING function
```
<character substring function> ::=
  SUBSTRING <left paren> <character value expression> FROM <start position>
      [ FOR <string length> ] [ USING CHARACTERS | OCTETS ] <right paren>
```
In YQL, it's supported as `SUBSTRING(str, start, len)`.
Character numbering within a string in YQL begins with zero

##### E021-09 TRIM function
```
 TRIM([[LEADING | TRAILING | BOTH] [<char value expression>] FROM] <char value expression>)
```
Deleting a prefix or suffix from a string. Not supported in YQL; you can build a functional equivalent using a String/Unicode UDF.

##### E021-11 POSITION function
```
POSITION(S1 IN S2) [USING CHARACTERS | OCTETS]
```
Searching a substring S1 in a string S2.

Supported in YQL in the format `FIND(S2, S1)`. Positions are numbered beginning from 0. If the string isn't found, NULL is returned.

#### E031 Identifiers
According to the standard, the IDs (for example, table and column names) can be
regular identifiers and delimited identifiers.
Regular identifiers begin with a letter and can only include letters, digits, and underscores.


##### E031-01 Delimited identifiers
Delimited identifiers are enclosed in double quotes: `"abc def"` and can contain arbitrary characters.
Double quotes are escaped by double quotes: `"complex ""identifier"""`

Instead of double quotes, YQL uses backticks

#### E051 Basic query specification
According to the standard, the generic format of the SELECT statement is as follows
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

YQL doesn't support multiple tables after `FROM` (`<table reference list>` includes more than one item)

##### E051-01 SELECT DISTINCT

SELECT DISTINCT is supported except for the variant with asterisk/qualified asterisk

##### E051-07 Qualified * in select list

In YQL:
* `<asterisk identifier chain>` can only consist of one item
* `<All fields reference>` doesn't support aliases `<all fields column name list>`
* `<Value expression primary>` can only include a link to a table


##### E061-01 Comparison predicate

YQL supports operators  <,>,=,<>,<=,>= for compatible types

##### E061-02 BETWEEN predicate

```
<between predicate> ::=
  <row value predicand> <between predicate part 2>
<between predicate part 2> ::=
  [ NOT ] BETWEEN [ ASYMMETRIC | SYMMETRIC ]
      <row value predicand> AND <row value predicand>
```

YQL doesn't support SYMMETRIC/ASSYMETRIC: `A BETWEEN B AND C` is equivalent to `A >= B AND A <= C`.

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
