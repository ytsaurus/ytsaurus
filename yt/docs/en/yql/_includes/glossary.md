#|
|| **Term** | **English translation**  | **Description** ||
|| Query | Query | Program text in YQL ||
|| Operation | Operation | Query execution process. In YQL terminology, a query and an operation relate to each other much like a program and a process do in an operating system. ||
|| Table | Table | Logically, a table is a list of structures (`List<Struct<...>>`) that is a basic primitive for processing large volumes of data in YQL.

List items form rows, while structure members serve as table cells. Vertically aligned cells constitute a table column. ||
|| Expression | Expression | A computed value. Typically, it takes one or more table cells as input, with the output becoming a cell in another table. Examples:
- `2 * 2;`
- `String::ToUpper("hi, " \|\| name \|\| "!")`
||
|| Statement | Statement | Query components separated by semicolons and starting with a verb. Examples:
- `SELECT ...;`
- `INSERT INTO ... SELECT ... ;`
- `REDUCE ... USING ... ON ...;`
||
|| Subquery | Subquery | A query component that, similarly to tables, can be used as input for statements or other subqueries. Optionally, subqueries can be [parameterized](../syntax/subquery.md) ||
|| Named node | Named node | A mechanism for reusing expressions and subqueries within one query. [More](../syntax/expressions.md#named-nodes)

Examples:
- `$foo = 2 + 2;`
- `$bar = (SELECT bar FROM my_table);` ||
|| Lambda | Lambda | A parameterizable block consisting of one or more named nodes (specifically expressions, not statements) where the result of the last node becomes the result of the entire lambda call. The call is made by passing parameters in parentheses. [Learn more](../syntax/expressions.md#lambda) ||
|| UDF (User-Defined Function) | UDF (User-Defined Function)  | Functions that let you integrate business logic into a query using one of the supported popular programming languages.

[C++](../udf/cpp.md) UDFs are loaded in compiled `.so` format, with a wide range of [built-in](../udf/list/pcre.md) functions available as well. [Python](../udf/python.md){% if audience == "internal" %} and [JavaScript](../udf/javascript.md){% endif %} UDFs can use a script from any query string, but require additional description of their signature.

Since the YQL optimizer can't peek inside the {% if audience == "internal" %}JavaScript engine (V8) or {% endif %}Python interpreter, **we recommend using lambda functions and C++ UDFs** for better performance. ||
|| Action | Action | A parameterizable block consisting of one or multiple statements that can then be invoked any number of times using special keywords. Unlike lambda functions, they **don't** return any result. [Learn more](../syntax/action.md) ||
|| Library | Library | A query component stored in a separate file for reuse or convenience. [Learn more](../syntax/export_import.md) ||
|#

