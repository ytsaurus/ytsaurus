# Запросы, синтаксис, UDF

## Как сделать запрос по логам за последний день/неделю/месяц или какой-либо другой относительный период времени?
Если подставлять даты в текст запроса перед запуском неудобно, то можно сделать следующим образом:

1. Пишем UDF на [Python](../udf/python.md){% if audience == "internal" %}, [JavaScript](../udf/javascript.md){% endif %} или [C++](../udf/cpp.md) с сигнатурой `(String)->Bool`. По имени таблицы она должна вернуть true только для попадающих в нужный диапазон, ориентируясь на текущее время или дату, полученные средствами выбранного языка программирования.
2. Передаём эту UDF в функцию [FILTER](../syntax/select/index.md#func_table) вместе с префиксом до директории с логами.
3. Используем результат работы FILTER в любом месте, где ожидается путь к входным данным ([FROM](../syntax/select/from.md), [PROCESS](../syntax/process.md), [REDUCE](../syntax/reduce.md))

{% if audience == "internal" %}[Пример](https://nda.ya.ru/t/Qa3wDKXV7FVHVJ).{% endif %}

Так как форматы даты и времени в названиях таблиц могут быть произвольными, а также логика выбора подходящих таблиц может быть неопределённо сложной, более специализированного решения для этой задачи не предоставляется.

## Как сделать фильтрацию по большому списку значений?
Можно, конечно, сгенерировать гигантское условие в `WHERE`, но лучше:

* Сгенерировать текстовый файл со списком значений по одному на строку, а затем приложить его к запросу и воспользоваться [IN](../syntax/expressions.md#in) в сочетании с функцией [ParseFile](../builtins/basic.md#parsefile).
* Сформировать таблицу из значений, по которым нужно сделать фильтрацию, и выполнить `LEFT SEMI JOIN` с этой таблицей.
* Для строк можно сгенерировать регулярное выражение, проверяющее их через «или», и воспользоваться ключевым словом [REGEXP](../syntax/expressions.md#regexp), либо одной из предустановленных функций для работы с регулярными выражениями ([Hyperscan](../udf/list/hyperscan.md), [Pire](../udf/list/pire.md) или [Re2](../udf/list/re2.md)).

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
Это виртуальная колонка. Она создаётся только YQL-движком (система {{product-name}} про неё ничего не знает) и только для таблиц с [нестрогой схемой](*schema-not-strict). В этой колонке доступны столбцы таблицы, которые изначально в схеме не были заданы. Колонка имеет тип `Dict<String,String>`.

Есть несколько распространённых способов получить таблицу, в которой есть такая колонка:
- Например, создать таблицу без схемы и затем отсортировать её. Система {{product-name}} выставит отсортированной таблице схему, в которой будут явно указаны только имена столбцов, по которым производилась сортировка. А если прочитать сортированную таблицу в YQL, то остальные столбцы будут доступны в поле `_other`.
- Другой способ &mdash; создать таблицу с нестрогой схемой и добавить в эту таблицу колонки, которые изначально не были явно указаны в схеме. Ниже приведён пример этого кейса.

Пример:

```bash
# Создадим таблицу с нестрогой схемой. В схеме явно заданы две колонки: 'id' и 'text'.
$ yt create table //tmp/table_not_strict --attributes '{schema = <strict=%false>[{name = id; type = int64}; {name = text; type = string}]}'

# Теперь добавим в таблицу данные, указав дополнительную колонку 'newColumn', которой нет в схеме.
$ echo '{ "id": 0, "text": "Hello, World!", "newColumn": "This column is not in schema" } ' | yt write-table //tmp/table_not_strict --format json
```

{% note info %}

Создавать таблицы с нестрогой схемой можно только через {{product-name}} SDK. В веб-интерфейсе это сделать не получится.

{% endnote %}

Если прочитать эту таблицу в YQL, поле `newColumn` будет доступно в колонке `_other`:

``` yql
SELECT * FROM `//tmp/table_not_strict` AS t;
```

![](../../../images/yql_other_field.png)

Получить значение колонки `newColumn` можно несколькими способами:

```yql
SELECT
    WeakField(t.newColumn, "String")
FROM `//tmp/table_not_strict` AS t;
```
или

``` yql
SELECT
    t._other["newColumn"]
FROM `//tmp/table_not_strict` AS t;
```

Первый способ позволяет работать единообразно с таблицей, если вдруг колонка `newColumn` попадет в строгую схему. Также [WeakField](../builtins/basic.md#weakfield) позволяет сразу получить значение в нужном типе, а при работе с `_other` полученные значения пришлось бы обрабатывать как Yson. Для случая строгой схемы проверяется, что тип, переданный в WeakField, совпадает с типом в схеме.

{% note warning %}

Если результат запроса `SELECT * FROM <таблица с виртуальной колонкой _other>` записать в другую таблицу, то в новой таблице колонка `_other` превратится уже в реальную колонку. Но схема этой таблицы тогда будет строгая.

Если в YQL пытаться прочитать нестрогую таблицу, в которой уже есть реальная колонка `_other`, то возникнет ошибка: "Non-strict schema contains '_other' column, which conflicts with YQL virtual column".

{% endnote %}

<!--[Подробнее](../misc/schema.md)-->

## Как создать пустую таблицу со схемой из другой таблицы?

``` yql
-- Определяем тип записи исходных данных
$s = select * from `path/to/source_table`;
$s = process $s;

-- Создаём пустую таблицу с нужной схемой
INSERT INTO `path/to/my_destination_table` WITH TRUNCATE
SELECT * FROM as_table(ListCreate(ListItemType(TypeOf($s))))
```

## Как не распаковывать protobuf, а работать со строкой?
YQL умеет распаковывать protobuf сообщения. Рекомендуется настраивать <!--[автоматическую распаковку](../misc/schema.md#_yql_proto_field)--> автоматическую распаковку через метаданные таблиц. В некоторых случаях вместо распакованного сообщения необходимо работать с исходной строкой. Для этого стоит воспользоваться специальным представлением [raw](../syntax/select/index.md#view):

``` yql
SELECT *
FROM `path/to/my_table` VIEW raw;
```

{% note info "Примечание" %}

Стоит учитывать, что мета-атрибут автораспаковки таблиц, созданных таким запросом, скопирован не будет, его необходимо устанавливать заново.

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

-- Создаём список атрибутов для копирования
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

-- Форсируем скалярный контекст
$attributes = EvaluateExpr($attributes);

INSERT INTO $output WITH (TRUNCATE, USER_ATTRS=$attributes)
SELECT *
FROM (
    SELECT *
    FROM $input
    LIMIT 5
)
```

{% if audience == "internal" %}[Пример запроса](https://nda.ya.ru/t/Ro7bWr5H7FVHXX){% endif %}

## JSON и YSON

### Как превратить строку таблицы в JSON?
Для этого потребуются предустановленные [функции](../udf/list/yson.md) сериализации и стандартная функция [TableRow()](../builtins/basic.md#tablerow) для обращения ко всей строке:
``` yql
USE {{production-cluster}};
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
FROM {{production-cluster}}.`home/yql/tutorial/users` VIEW complex
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
FROM {{production-cluster}}.`home/yql/tutorial/users_dsv` AS dsv
FLATTEN DICT BY dict;
```

Подробнее про конструкцию `FLATTEN BY` можно прочитать в соответствующем [разделе](../syntax/flatten.md).

## Как передать данные одной таблицы (справочника) в виде списка для обработки другой таблицы {#list-table-attach}

Если размер таблицы справочника не превышает лимита на размер одной строки таблицы, то можно воспользоваться скалярным контекстом и функцией `AGGREGATE_LIST`.

``` yql
$data = SELECT AGGREGATE_LIST(TableRow()) FROM small_table; -- скалярный контекст при использовании $data в выражении, тип Optional<List>

SELECT MyModule::MyUdf($data ?? [])(x) FROM big_table;
```

{% if audience == "internal" %}[Пример запроса](https://nda.ya.ru/t/OQJ9nMKC7FVHYu){% endif %}

Если размер таблицы справочника побольше, но она все еще может быть загружена в каждую джобу целиком, то можно воспользоваться табличным контекстом.

``` yql
$data = SELECT * FROM medium_table;
$data = PROCESS $data; -- включаем табличный контекст, $data будет далее иметь тип List<Struct>

SELECT MyModule::MyUdf($data)(x) FROM big_table;
```
{% if audience == "internal" %}[Пример запроса](https://nda.ya.ru/t/2aFoAR0E7FVHa9){% endif %}

[*schema-not-strict]: У такой схемы выставлен атрибут `<strict=%false>`. [Подробнее про схемы таблицы](../../user-guide/storage/static-schema.md)
