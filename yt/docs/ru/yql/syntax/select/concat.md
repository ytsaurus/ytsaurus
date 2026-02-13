# Обращение к нескольким таблицам в одном запросе

В стандартном SQL для выполнения запроса по нескольким таблицам используется [UNION ALL](../select/operators.md#union-all), который объединяет результаты двух и более `SELECT`. Это не совсем удобно для сценария использования, в котором требуется выполнить один и тот же запрос по нескольким таблицам (например, содержащим данные на разные даты). В YQL, чтобы было удобнее, в `SELECT` после `FROM` можно указывать не только одну таблицу или подзапрос, но и вызывать встроенные функции, позволяющие объединять данные нескольких таблиц.

Для этих целей определены следующие функции:

```CONCAT(`table1`, `table2`, `table3` VIEW view_name, ...)``` &mdash; объединяет все перечисленные в аргументах таблицы.

```EACH($list_of_strings) или EACH($list_of_strings VIEW view_name)``` &mdash; объединяет все таблицы, имена которых перечислены в списке строк. Опционально можно передать несколько списков в отдельных аргументах по аналогии с `CONCAT`.

```RANGE(`prefix`, `min`, `max`, `suffix`, `view`)``` &mdash; объединяет диапазон таблиц. Аргументы:

* prefix &mdash; каталог для поиска таблиц, указывается без завершающего слеша. Единственный обязательный аргумент, если указан только он, то используются все таблицы в данном каталоге.
* min, max &mdash; следующие два аргумента задают диапазон имен для включения таблиц. Диапазон инклюзивный с обоих концов. Если диапазон не указан, используются все таблицы в каталоге prefix. Имена таблиц или директорий, находящихся в указанной в prefix директории, сравниваются с диапазоном `[min, max]` лексикографически, а не конкатенируются, таким образом важно указывать диапазон без лидирующих слешей.
* suffix &mdash; имя таблицы. Ожидается без начального слеша. Если suffix не указан, то аргументы `[min, max]` задают диапазон имен таблиц. Если suffix указан, то аргументы `[min, max]` задают диапазон папок, в которых существует таблица с именем, указанным в аргументе suffix.

```LIKE(`prefix`, `pattern`, `suffix`, `view`)``` и ```REGEXP(`prefix`, `pattern`, `suffix`, `view`)``` &mdash; аргумент pattern задается в формате, аналогичном одноименным бинарным операторам: [LIKE](../expressions.md#like) и [REGEXP](../expressions.md#regexp).

```FILTER(`prefix`, `callable`, `suffix`, `view`)``` &mdash; аргумент callable должен являться вызываемым выражением с сигнатурой `(String)->Bool`, который будет вызван для каждой таблицы/подкаталога в каталоге prefix. В запросе будут участвовать только те таблицы, для которых вызываемое значение вернуло `true`. В качестве вызываемого значения удобнее всего использовать [лямбда функции](../expressions.md#lambda), либо UDF на [Python](../../udf/python.md){% if audience == "internal" %} или [JavaScript](../../udf/javascript.md){% endif %}.

```PARTITION_LIST($list_of_structs)``` &mdash; объединяет все таблицы, которые перечислены в списке структур, и добавляет к этим таблицам новые колонки. В структуре должно быть поле `TablePath` (путь к таблице). Поле `TablePath` должно иметь тип `String` или `Utf8`. Также поддерживается необязательное поле `TableView` c типом `Optional<String>` или `Optional<Utf8>`. Если поле `TableView` не null, то к таблице применяется соответствующий [VIEW](../select/view.md).
Все остальные поля структуры добаляются в виде дополнительных колонок к соответствующей таблице.
При этом сначала применяется `VIEW`/автораспаковка protobuf<!--[VIEW](../../misc/schema.md#_yql_view)/[автораспаковка protobuf](../../misc/schema.md#_yql_proto_field)/[read_udf](../../misc/schema.md#yqlreadudf)-->, а потом добавляются колонки из `PARTITION_LIST`.
Добавляемые колонки заменяют одноименные колонки из таблицы.
Функция PARTITION_LIST доступна начиная с версии [2025.04](../../changelog/2025.04.md).

```PARTITIONS(`prefix`, `pattern`, `view`)``` &mdash; объединяет таблицы, подпадающие под шаблон `pattern`, и добавляет части путей соответствующих компонентам шаблона к таблице как колонки. Аргументы:

* prefix &mdash; каталог для поиска таблиц, указывается без завершающего слеша
* pattern &mdash; шаблон, который будет применяться к путям таблиц без начального prefix. Шаблон может содержать следующие компонеты:
  * '*' – матчит 0 или более символов пути не включая `/`. Поведение аналогично поведению `*` в Unix shell
  * ${name:type} &mdash; матчит часть компонента пути соответствующую указанному типу и создает дополнительную колонку в таблице с типом type и именем name. Если тип не указан, то подразумевается String.
  * все остальные символы матчат сами себя
* view – необязательный аргумент. Задает VIEW для всех таблиц соответствующих шаблону.
Функция PARTITIONS доступна начиная с версии [2025.04](../../changelog/2025.04.md).

Примеры по использованию этих функций смотрите ниже.

{% note warning %}

Порядок, в котором будут объединены таблицы, всеми вышеперечисленными функциями не гарантируется.

Список таблиц вычисляется **до** запуска самого запроса. Поэтому созданные в процессе запроса таблицы не попадут в результаты функции.

Для функций `CONCAT`/`RANGE`/`LIKE`/`REGEXP`/`FILTER`/`PARTITIONS` к аргументу prefix автоматически добавляется [TablePathPrefix](../pragma/global.md#table-path-prefix) в случае, если prefix не является абсолютным путем.
Для функций `EACH` и `PARTITION_LIST` этого не происходит – для того чтобы [TablePathPrefix](../pragma/global.md#table-path-prefix) действовал и на них, необходимо включить PRAGMA [UseTablePrefixForEach](../pragma/global.md#use-table-prefix-for-each).

Колонки, добавляемые через `PARTITIONS`/`PARTITION_LIST`, заменяют одноименные колонки из исходной таблицы. Рекомендуется использовать уникальные имена для избежания конфликтов.

{% endnote %}

По умолчанию схемы всех участвующих таблиц объединяются по правилам [UNION ALL](operators.md#union-all). Если объединение схем не желательно, то можно использовать функции с суффиксом `_STRICT`, например `CONCAT_STRICT`, которые работают полностью аналогично оригинальным, но считают любое расхождение в схемах таблиц ошибкой.

Для указания кластера объединяемых таблиц нужно указать его перед названием функции.

Все аргументы описанных выше функций могут быть объявлены отдельно через [именованные выражения](../expressions.md#named-nodes). В этом случае в них также допустимы и простые выражения посредством неявного вызова [EvaluateExpr](../../builtins/basic.md#evaluate_expr_atom).

Имя исходной таблицы, из которой изначально была получена каждая строка, можно получить при помощи функции [TablePath()](../../builtins/basic.md#tablepath).

## Примеры

```yql
USE some_cluster;
SELECT * FROM CONCAT(
  `table1`,
  `table2`,
  `table3`);
```

```yql
USE some_cluster;
$indices = ListFromRange(1, 4);
$tables = ListMap($indices, ($index) -> {
    RETURN "table" || CAST($index AS String);
});
SELECT * FROM EACH($tables); -- идентично предыдущему примеру
```

```yql
USE some_cluster;
SELECT * FROM RANGE(`my_folder`);
```

```yql
SELECT * FROM some_cluster.RANGE( -- Кластер можно указать перед названием функции
  `my_folder`,
  `from_table`,
  `to_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_folder`,
  `to_folder`,
  `my_table`);
```

```yql
USE some_cluster;
SELECT * FROM RANGE(
  `my_folder`,
  `from_table`,
  `to_table`,
  ``,
  `my_view`);
```

```yql
USE some_cluster;
SELECT * FROM LIKE(
  `my_folder`,
  "2017-03-%"
);
```

```yql
USE some_cluster;
SELECT * FROM REGEXP(
  `my_folder`,
  "2017-03-1[2-4]?"
);
```

```yql
$callable = ($table_name) -> {
    return $table_name > "2017-03-13";
};

USE some_cluster;
SELECT * FROM FILTER(
  `my_folder`,
  $callable
);
```

```yql
$part_list = AsList(
  AsStruct("path/to/table1" as TablePath, "view1" as TableView, 1 as idx, "1" as idx_str),
  AsStruct("path/to/table2" as TablePath,                       2 as idx, "2" as idx_str),
);

SELECT * FROM some_cluster.PARTITION_LIST($part_list);

-- запрос выше эквивалентен
-- SELECT 1 as idx, "1" as idx_str, t1.* WITHOUT idx, idx_str FROM some_cluster.`path/to/table1` AS t1
-- UNION ALL
-- SELECT 2 as idx, "2" as idx_str, t2.* WITHOUT idx, idx_str FROM some_cluster.`path/to/table2` AS t2
```

```yql
-- предположим, что директория //home/daily_log имеет следущую структуру: //home/daily_logs/<дата в формате YYYY-MM-DD>/<название компании>/event_log
use some_cluster;

pragma yt.EarlyPartitionPruning;

SELECT company, event_id, count(*) as events_count
FROM PARTITIONS(`//home/daily_logs`, "${event_date:Date}/${company}/event_log")
-- с yt.EarlyPartitionPruning YQL проанализирует предикат в WHERE и исключит все таблицы старше 2025-01-01.
-- для исключенных таблиц не будут загружаться метаданные.
WHERE event_date >= Date("2025-10-01") OR event_id = 123 AND event_date >= Date("2025-01-01")
GROUP BY company, event_id;
```
