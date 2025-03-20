# SELECT

Возвращает результат вычисления выражений, указанных после `SELECT`. Может использоваться в сочетании с другими операциями для получения иного эффекта.

Примеры:

```yql
SELECT "Hello, world!";
```

```yql
SELECT 2 + 2;
```

## Процедура выполнения SELECT {#selectexec}

Результат запроса `SELECT` вычисляется следующим образом:

* определяется набор входных таблиц – вычисляются выражения после [FROM](from.md);
* к входным таблицам применяется [SAMPLE](sample.md) / [TABLESAMPLE](sample.md)
* выполняется [FLATTEN COLUMNS](../flatten.md#flatten-columns) или [FLATTEN BY](../flatten.md); алиасы, заданные во `FLATTEN BY`, становятся видны после этой точки;
* выполняются все [JOIN](../join.md);
* к полученным данным добавляются (или заменяются) колонки, заданные в [GROUP BY ... AS ...](../group_by.md);
* выполняется [WHERE](where.md) &mdash; все данные не удовлетворяющие предикату отфильтровываются;
* выполняется [GROUP BY](../group_by.md), вычисляются значения агрегатных функций;
* выполняется фильтрация [HAVING](../group_by.md#having);
* вычисляются значения [оконных функций](../window.md);
* вычисляются выражения в `SELECT`;
* выражениям в `SELECT` назначаются имена заданные алиасами;
* к полученным таким образом колонкам применяется top-level [DISTINCT](distinct.md);
* таким же образом вычисляются все подзапросы в [UNION ALL](union.md#union-all), выполняется их объединение (см. [PRAGMA AnsiOrderByLimitInUnionAll](../pragma.md#pragmas));
* выполняется сортировка согласно [ORDER BY](order-by.md);
* к полученному результату применяются [OFFSET и LIMIT](limit-offset.md).

## Порядок колонок в YQL {#orderedcolumns}

В стандартном SQL порядок колонок указанных в проекции (в `SELECT`) имеет значение. Помимо того, что порядок колонок должен сохраняться при отображении результатов запроса или при записи в новую таблицу, некоторые конструкции SQL этот порядок используют.
Это относится в том числе к [UNION ALL](union.md#union-all) и к позиционному [ORDER BY](order-by.md) (ORDER BY ordinal).

По умолчанию в YQL порядок колонок игнорируется:

* порядок колонок в выходных таблицах и в результатах запроса не определен
* схема данных результата `UNION ALL` выводится по именам колонок, а не по позициям

При включении `PRAGMA OrderedColumns;` порядок колонок сохраняется в результатах запроса и выводится из порядка колонок во входных таблицах по следующим правилам:

* `SELECT` с явным перечислением колонок задает соответствующий порядок;
* `SELECT` со звездочкой (`SELECT * FROM ...`) наследует порядок из своего входа;
* порядок колонок после [JOIN](../join.md): сначала колонки левой стороны, потом правой. Если порядок какой-либо из сторон присутствующей в выходе `JOIN` не определен, порядок колонок результата также не определен;
* порядок `UNION ALL` зависит от режима выполнения [UNION ALL](union.md#union-all);
* порядок колонок для [AS_TABLE](from-as-table.md) не определен;


### Комбинация запросов {#combining-queries}

Результаты нескольких SELECT (или подзапросов) могут быть объединены с помощью ключевых слов `UNION` и `UNION ALL`.

```yql
query1 UNION [ALL] query2 (UNION [ALL] query3 ...)
```

Объединение более двух запросов интерпретируется как левоассоциативная операция, то есть

```yql
query1 UNION query2 UNION ALL query3
```

интерпретируется как

```yql
(query1 UNION query2) UNION ALL query3
```

При наличии `ORDER BY/LIMIT/DISCARD/INTO RESULT` в объединяемых подзапросах применяются следующие правила:

* `ORDER BY/LIMIT/INTO RESULT` допускается только после последнего подзапроса;
* `DISCARD` допускается только перед первым подзапросом;
* указанные операторы действуют на результат `UNION [ALL]`, а не на подзапрос;
* чтобы применить оператор к подзапросу, подзапрос необходимо взять в скобки.

## Поддерживаемые конструкции в SELECT

* [FROM](from.md)
* [FROM AS_TABLE](from-as-table.md)
* [FROM SELECT](from-select.md)
* [DISTINCT](distinct.md)
* [UNIQUE DISTINCT](unique-distinct-hints.md)
* [UNION](union.md)
* [WITH](with.md)
* [WITHOUT](without.md)
* [WHERE](where.md)
* [ORDER BY](order-by.md)
* [ASSUME ORDER BY](assume-order-by.md)
* [LIMIT OFFSET](limit-offset.md)
* [SAMPLE](sample.md)
* [TABLESAMPLE](sample.md)
* [FOLDER](folder.md)
* [WalkFolders](walk-folders.md)
* [VIEW](view.md)
* [TEMPORARY TABLE](temporary-tables.md)
* [CONCAT](concat.md)

