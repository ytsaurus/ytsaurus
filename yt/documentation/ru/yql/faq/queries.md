---
vcsPath: yql/docs_yfm/docs/ru/yql-product/faq/queries.md
sourcePath: yql-product/faq/queries.md
---
# Запросы, синтаксис, UDF

## Как сделать запрос по логам за последний день/неделю/месяц или какой-либо другой относительный период времени?
Если подставлять даты в текст запроса перед запуском не удобно, то можно сделать следующим образом:

1. Пишем UDF на [Python](../udf/python.md), [JavaScript](../udf/javascript.md) или [C++](../udf/cpp.md) с сигнатурой `(String)->Bool`. По имени таблицы она должна вернуть true только для попадающих в нужный диапазон, ориентируясь на текущее время или дату, полученные средствами выбранного языка программирования.
2. Передаём эту UDF в функцию [FILTER](../syntax/select.md#filter) вместе с префиксом до директории с логами.
3. Используем результат работы FILTER в любом месте, где ожидается путь к входным данным ([FROM](../syntax/select.md#from), [PROCESS](../syntax/process.md), [REDUCE](../syntax/reduce.md))

[Пример](https://cluster-name.yql/Operations/WXcgQY0UzHDE91SFjG2rUqDJc118T4EeE-33gDGw1ZE=).

Так как форматы даты и времени в названиях таблиц могут быть произвольными, а также логика выбора подходящих таблиц может быть не определённо сложной, более специализированного решения для этой задачи не предоставляется.

## Как сделать фильтрацию по большому списку значений?
Можно, конечно, сгенерировать гигантское условие в `WHERE`, но лучше:

* Сгенерировать текстовый файл со списком значений по одному на строку, а затем [приложить его к запросу](../interfaces/web.md#attach) и воспользоваться [IN](../syntax/expressions.md#in) в сочетании с функцией [ParseFile](../builtins/basic.md#parsefile).
* Сформировать таблицу из значений, по которым нужно сделать фильтрацию, и выполнить `LEFT SEMI JOIN` с этой таблицей.
* Для строк можно сгенерировать регулярное выражение, проверяющее их через «или» и воспользоваться ключевым словом [REGEXP](../syntax/expressions.md#regexp), либо одной из предустановленных функций для работы с регулярными выражениями ([Hyperscan](../udf/list/hyperscan.md), [Pire](../udf/list/pire.md) или [Re2](../udf/list/re2.md)).

## Чем отличаются MIN, MIN\_BY и MIN\_OF? (или MAX, MAX\_BY и MAX\_OF)

* `MIN(column)` — агрегатная функция из SQL-стандарта, возвращающая минимальное значение, найденное в указанной колонке.
* `MIN_BY(result, by)` — нестандартная агрегатная функция, которая, в отличие от предыдущей, возвращает не само минимальное значение, а какое-то другое значение из строк(-и), где нашелся минимум.
* `MIN_OF(a, b, c, d)` — встроенная функция, которая возвращает среди N значений-аргументов минимальное.

## Как обратиться к колонке, в имени которой есть знак минус или другой спецсимвол?
Обернуть имя колонки в backticks по аналогии с именами таблиц:
``` yql
SELECT `field-with-minus` FROM `table`;
```

## Что такое колонка \_other?
Это колонка, имеющая тип `Dict<String,String>`, в которой будут доступны столбцы таблицы, явно не заданные в нестрогой схеме (с атрибутом `<strict=%false>`).
Есть несколько распространенных способов получить такую таблицу. Например, если изначально таблица была создана без схемы ([в терминах {{product-name}}](../../user-guide/storage/static-schema.md)), а потом отсортирована, система ({{product-name}}) выставляет ей схему, в которой явно указаны только имена столбцов, по которым производилась сортировка.

Например, такая схема часто присутствует у сортированных таблиц, полученных через YaMR-обертку. В этом случае доступ к `value` можно получить следующим образом:

``` yql
SELECT
    WeakField(t.value, "String")
FROM `path/to/my_table` AS t;
```
или

``` yql
SELECT
    t._other["value"]
FROM `path/to/my_table` AS t;
```
Первый способ позволяет работать единообразно с таблицей, если вдруг поле value попадет в строгую схему. Также WeakField позволяет сразу получить значение в нужном типе, а при работе с _other полученные значения пришлось бы обрабатывать как Yson. Для случая строгой схемы проверяется, что тип переданный в WeakField совпадает с типом в схеме.

[Подробнее](../misc/schema.md)

## Как создать пустую таблицу со схемой из другой таблицы?

``` yql
-- Определяем тип записи исходных данных
$s = select * from `path/to/source_table`;
$s = process $s;

-- Создаем пустую таблицу с нужной схемой
INSERT INTO `path/to/my_destination_table` WITH TRUNCATE
SELECT * FROM as_table(ListCreate(ListItemType(TypeOf($s))))
```

## Как не распаковывать protobuf, а работать со строкой?
YQL умеет распаковывать protobuf сообщения. Рекомендуется настраивать [автоматическую распаковку](../misc/schema.md#_yql_proto_field) через метаданные таблиц. В некоторых случаях вместо распакованного сообщения необходимо работать с исходной строкой. Для этого стоит воспользоваться специальным представлением [raw](../syntax/select.md#view):

``` yql
SELECT *
FROM `path/to/my_table` VIEW raw;
```

{% note info "Примечание" %}

Стоит учитывать, что мета-атрибут автораспаковки таблиц, созданных таким запросом скопирован не будет, его необходимо устанавливать заново.

{% endnote %}

## Как превратить строку вида "a=b,c=d" в словарь?
Для этого достаточно применить предустановленную функцию из модуля [Dsv](../udf/list/dsv.md) UDF:

``` yql
$str = "a=b,c=d";
SELECT Dsv::Parse($str, ",");

/*
Результат:
{
    "a": "b",
    "c": "d"
}
*/
```

## Как удалить из таблицы дубликаты строк, не перечисляя все колонки?

``` yql
SELECT * WITHOUT g FROM (
    SELECT Some(TableRow()) AS s
    FROM `path/to/table` AS t
    GROUP BY Digest::Sha256(StablePickle(TableRow())) AS g
)
FLATTEN COLUMNS;
```

## Как скопировать пользовательские атрибуты на выходную таблицу

``` yql
$input = "some/input/path";
$output = "destination/path";

-- Фильтруем пользовательские атрибуы, начинающиеся с подчеркивания, и исключаем некоторые системные YQL атрибуты
$user_attr = ($attr) -> {
    RETURN ListFilter(
        Yson::ConvertToStringList($attr.user_attribute_keys),
        ($name) -> {
            RETURN StartsWith($name, "_") AND $name NOT IN ["_yql_op_id", "_yql_row_spec", "_yql_runner"];
        }
    );
};

-- Добавляем префикс // к пути
$yt_path = ($path) -> {
    RETURN IF(StartsWith($path, "//"), $path, "//" || $path);
};

-- Создаем список атрибутов для копирования
$attribute_keys = (
    SELECT String::JoinFromList($user_attr(Attributes), ";")
    FROM Folder(Substring($input, NULL, RFind($input, "/")), "user_attribute_keys")
    WHERE $yt_path(Path) == $yt_path($input)
);

-- Читаем пользовательские атрибуты
$attributes = (
    SELECT CAST(Yson::SerializePretty(Attributes) AS String) as attrs
    FROM Folder(Substring($input, NULL, RFind($input, "/")), $attribute_keys)
    WHERE $yt_path(Path) == $yt_path($input)
);

-- В списке должен получиться только один элемент
$attributes = EvaluateExpr(ListHead($attributes).attrs);

INSERT INTO $output WITH (TRUNCATE, USER_ATTRS=$attributes)
SELECT *
FROM (
    SELECT *
    FROM $input
    LIMIT 5
)
```

<<<<<<< trunk: link to future POM file
<!-- [Пример запроса](https://yql.cluster.domain.com/Operations/Yswkc5fFt1Qnb6_ULTyDPfOyMQKz2R34k5aPI1aZKFM=) -->
=======
<!--[Пример запроса](https://cluster-name.yql/Operations/Yswkc5fFt1Qnb6_ULTyDPfOyMQKz2R34k5aPI1aZKFM=)-->
>>>>>>> yandex-things: fix after review

## JSON и YSON

### Как превратить строку таблицы в JSON?
Для этого потребуются предустановленные [функции](../udf/list/yson.md) сериализации и стандартная функция [TableRow()](../builtins/basic.md#tablerow) для обращения ко всей строке:
``` yql
USE hahn;
SELECT Yson::SerializeJson(Yson::From(TableRow()))
FROM `home/yql/tutorial/users`;
```

### Как получить значение по ключу из JSON'а / YSON'а? {#yson-json}
При работе с JSON'ом / YSON'ом стоит помнить:

* Несериализованное поле этих форматов нельзя вывести в браузере или сохранить в таблицу — возникает ошибка вида `
  Expected persistable data, but got: List<Struct<'column0':Resource<'Yson.Node'>>>`;
* Не стоит преобразовывать в словарь, если нужно получить значение только по одному ключу.
``` yql
-- Тип my_column — Yson

SELECT Yson::Serialize(my_json_column.myKey) AS my_column
FROM my_table;
```
Если требуется конкретный тип данных, то можно воспользоваться функцией [Yson::ConvertTo...](../udf/list/yson.md#ysonconvertto):
``` yql
-- Тип my_column — String

SELECT Yson::ConvertToString(my_json_column.myKey) AS my_column
FROM my_table;
```
Если исходный тип колонки строка, то можно воспользоваться функцией [Yson::Parse...](../udf/list/yson.md#ysonparse):

``` yql
SELECT Yson::Serialize(Yson::Parse(my_yson_column)) AS my_yson;

-- В результате получается Yson, при работе с которым Yson::Parse подставляется автоматически

SELECT Yson::Serialize(my_yson.myKey) AS my_key_value;
```
Если требуется список из всех значений по ключу, то можно воспользоваться функцией [ListMap](../builtins/list.md#listmap):
``` yql
SELECT
    ListMap(
        Yson::ConvertToList(my_yson_column),
        ($x) -> { RETURN Yson::Serialize($x.foo) }
    );

-- Результат выполнения — список строк
```
Пример получения элемента списка:
``` yql
SELECT Yson::ConvertToString(Yson::ConvertToList(my_yson_column)[0].foo);

-- Результат выполнения — поле конкретного элемента списка
```

### Как вертикально развернуть словарь/список из таблицы? {#flatten}
В случае, когда контейнер, который необходимо развернуть — список, стоит использовать `FLATTEN LIST BY`:
``` yql
SELECT
    parents,
    name,
    geo_parents_list
FROM hahn.`home/yql/tutorial/users` VIEW complex
FLATTEN LIST BY geo_parents_list AS parents;
```

Если же список находится в колонке с типом Yson, то сначала необходимо преобразовать его в тип List. Например, так:
``` yql
SELECT *
FROM (
    SELECT
        ListMap(
            Yson::ConvertToList(my_yson_column),
            ($x) -> { RETURN Yson::Serialize($x.foo) }
        ) AS my_list
    FROM my_table)
FLATTEN LIST BY my_list;
```

`FLATTEN DICT BY` следует использовать, когда развернуть нужно словарь:
``` yql
SELECT
    dsv.dict.0 AS key,
    dsv.dict.1 AS value
FROM hahn.`home/yql/tutorial/users_dsv` AS dsv
FLATTEN DICT BY dict;
```

Подробнее про конструкцию `FLATTEN BY` можно прочитать в соответствующем [разделе](../syntax/flatten.md).
