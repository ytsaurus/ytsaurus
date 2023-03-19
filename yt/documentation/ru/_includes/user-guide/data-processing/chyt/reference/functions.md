# Функции

В данной статье собраны специфичные для CHYT функции.

## Функции для работы с YSON { #yson_functions }

В CHYT введены вспомогательные функции, которые выглядят как `YPathTYPENAME[Strict](yson, ypath)`, где `TYPENAME` это один из 9 возможных вариантов: `Int64`, `UInt64`, `Boolean`, `Double`, `String`, `ArrayInt64`, `ArrayUInt64`, `ArrayBoolean`, `ArrayDouble`, а суффикс `Strict` означает строгость функции.

Принцип работы функции состоит в следующем: 

1. Выбирается строка, содержащаяся в аргументе `yson` как YSON-документ.
2. Происходит адресация в этом документе по пути `ypath` относительно корня документа.
3. Происходит интерпретация содержимого документа по пути `ypath` к типу `TYPENAME`.

На каждом из этапов работы данной функции могут возникнуть ошибки:

1. Строка может не представлять собой YSON-документ (такое возможно при указании строковой константы в качестве аргумента функции, но не при чтении реальных Any-колонок из {{product-name}}).
2. Данный путь может отсутствовать в документе.
3. Данный путь может соответствовать иному типу данных, отличному от `TYPENAME` (например, вместо числового значения находится строковый литерал).

Отличие `Strict`-версии функции от `non-Strict`-версии заключается в режиме обработки данных ошибок: `Strict`-версия аварийно завершит работу запроса, тогда как `non-Strict`-версия вернет в качестве результата  значение по умолчанию для соответствующего типа колонки (`Null`, `0`,` пустую строку или пустой список) и продолжит работу.

Ниже представлены примеры. В качестве первого аргумента везде фигурирует строковый литерал, содержащий YSON, но на его месте может быть любая `Any`-колонка таблицы.


```sql
SELECT YPathInt64('{key1=42; key2=57; key3=29}', '/key2')
57

SELECT YPathInt64('[3;4;5]', '/1')
4

SELECT YPathString('["aa";"bb";"cc"]', '/1')
"bb"

SELECT YPathString('{a={[{c=xyz}; {c=qwe}]}}', '/a/1/c')
"qwe"

SELECT YPathInt64('{key=xyz}', '/key')
0

SELECT YPathInt64Strict('{key=xyz}', '/key')
std::exception. Code: 1001, type: NYT::TErrorException, e.what() = Node /key2 has invalid type: expected one of {"int64", "uint64"}, actual "string"
    code            500
    origin          ...

SELECT YPathString('{key=3u}', '/nonexistentkey')
""

SELECT YPathArrayInt64('[1;2;3]', '')
[1, 2, 3]

SELECT(YPathUInt64('42u', '')
42 
```

Предположим, что в таблице есть две колонки `lhs` и `rhs`, представляющие собой `Any`-колонки, содержащие списки чисел одинаковой длины. 

```sql
| lhs       | rhs       |
| --------- | --------- |
| [1, 2, 3] | [5, 6, 7] |
| [-1, 1]   | [1, 3]    |
| []        | []        |
```

Тогда можно построить скалярное произведение векторов `lhs` и `rhs` с помощью конструкции, представленной ниже.


```sql
SELECT arraySum((x, y) -> x*y, YPathArrayInt64(lhs, ''), YPathArrayInt64(rhs, '')) FROM "//path/to/table"
38
2
0
```

Также существуют функции `YPathRaw[Strict](yson, ypath, [format])`, которые кроме двух обязательных имеют третий опциональный параметр `format`(по умолчанию равен `binary`).
Данные функции возвращают всё поддерево в виде YSON-строки в запрошенном формате.

Для сложных типов поддержана функция `YPathExtract[Strict](yson, ypath, type)`, принимающая возвращаемый тип в виде строки третьим параметром.

Примеры:

```sql
SELECT YPathRaw('{x={key=[1]}}', '/x', 'text')
'{"key"=[1;];}'
SELECT YPathExtract('{x=["abc";"bbc"]}', '/x', 'Array(String)')
["abc", "bbc"]
SELECT YPathExtract('["abc";"bbc"]', '', 'Array(String)')
["abc", "bbc"]
```

Чтобы сделать работу с JSON в ClickHouse и YSON в CHYT похожей, были поддержаны аналоги всех функций из оригинального ClickHouse, которые начинаются со слова `JSON`.
Подробнее про семантику данных функции можно прочитать в [документации ClickHouse](https://clickhouse.com/docs/ru/sql-reference/functions/json-functions/).
Все YSON-аналоги отличаются от JSON-функций в названии только первой буквой (то есть имееют вид `YSON*` вместо `JSON*`).

{% note warning "Внимание" %}

В отличие от функций семейства `YPath*`, данные функции используют нумерацию с 1, а не с 0.
Кроме того, порядок полей в словарях в YSON формате не определен, и, соответсвенно, обращение к элементу словаря по индeксу не поддерживается.

{% endnote %}

### Форматы представлени YSON { #formats }

Данные из колонок типа `Any` могут храниться в бинарном представлении `YSON`. Поскольку читать такие данные не всегда удобно, в CHYT имеется функция `ConvertYSON`, которая преобразует разные представления `YSON` между собой.

`ConvertYson(YsonString, format)`

Всего существует три возможных формата представления:

- `binary`;
- `text`;
- `pretty`.

Примеры:

```sql
SELECT ConvertYson('{x=1}', 'text')
'{"x"=1;}'

SELECT ConvertYson('{x=1}', 'pretty')
'{
    "x" = 1;
}'

SELECT ConvertYson('{x=1}', 'binary')
'{\u0001\u0002x=\u0002\u0002;}'
```

### Примеры работы с YSON { #examples }

```sql
SELECT operation_id, task_id, YPathInt64(task_spec, '/gpu_limit') as gpu_limit, YPathInt64(task_spec, '/job_count') as job_count FROM (
    SELECT tupleElement(tasks, 1) as task_id, tupleElement(tasks, 2) as task_spec, operation_id FROM ( 
        SELECT operation_id, tasks FROM (
            Select YSONExtractKeysAndValuesRaw(COALESCE(tasks, '')) as tasks, operation_id FROM (
                SELECT YSONExtractRaw(spec, 'tasks') as tasks, operation_id
                FROM `//home/dev/chyt_examples/completed_ops`
                WHERE YSONHas(spec, 'tasks')
            )
        )
        ARRAY JOIN tasks
    )
)
ORDER BY gpu_limit DESC;
```

## Функции для получения версии { #version_functions }

- Получение версии CHYT
```sql
SELECT chytVersion()
```
- Получение версии {{product-name}}
```sql
SELECT ytVersion()
```

