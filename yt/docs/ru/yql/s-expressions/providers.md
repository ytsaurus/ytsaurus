# Провайдеры данных

## Config

Провайдер для работы с конфигурацией системы.
Поддерживается в форме DataSource.
Формат:
``` lisp
(DataSource 'config)
```
### Чтение списка кластеров

``` lisp
(let x (Read! world config (Key '('clusters)) '() '()))
```
Тип результата: Tuple of (World, Dict of (String, List of String)))
Ключ словаря — категория провайдера, список — список кластеров внутри этого провайдера.
Результат совместим только с провайдером result.

{% cut "Пример полной программы" %}

``` lisp
(
(let config (DataSource 'config))
(let x (Read! world config (Key '('clusters)) '() '()))
(let world (Left! x))
(let clusters (Right! x))
(let res_sink (DataSink 'result))
(let world (Write! world res_sink (Key) clusters '()))
(let world (Commit! world res_sink))
(return world)
)
```

{% endcut %}
{% cut "Пример выдачи" %}

```
[
    {
        "Write" = [
            {
                "Data" = {
                    "yamr" = [
                        "abies";
                        "betula"
                    ]
                }
            }
        ]
    }
]
```

{% endcut %}

{% if yt %}
## YT

Провайдер для работы с кластером YT.
Поддерживается в форме DataSource и DataSink.

Формат:
``` lisp
(DataSource 'yt 'clusterName)
(DataSink 'yt 'clusterName)
```
clusterName — описывает имя кластера, в конфигурации ему будет сопоставлен хост и порт.

### Чтение таблицы (одной или нескольких)

``` lisp
(let x (Read! world mr_source (Key '('table (String 'Input))) '('key 'subkey 'value) '()))
(let x (Read! world mr_source '((Key '('table (String 'Input1))) (Key '('table (String 'Input2)))) '('key 'subkey 'value) '()))
```
Тип результата: Tuple of (World, List of Struct (Fields))
Поля: key : String, subkey : String, value : String или (Void).


* Если вместо кортежа (Tuple) со списком полей указан литерал Void, то будут прочитаны все поля.
* Если в списке полей указано подмножество существующих полей, то в выходных данных чтения таблицы будут использованы только они.
* Если указано несколько таблиц, то производится их конкатенация. Схемы всех таблиц должны совпадать.

### Запись таблицы

Пример:
``` lisp
(let world (Write! world mr_sink (Key '('table (String 'Output))) table1 '('('mode 'append))))
```
Требуемый тип данных для записи: List of Struct (fields).
Требуемые поля: key : String, subkey : String, value : String.

Поддерживаемые опции:

* замена таблицы '() или '('('mode 'replace))
* добавление в конец '('('mode 'append))

### Удаление таблицы

Пример:
``` lisp
(let world (Write! world mr_sink (Key '('table (String 'Output))) (Void) '('('mode 'drop))))
```
### Чтение метаданных таблицы

Пример:
``` lisp
(let x (Read! world mr_source (Key '('tablescheme (String 'Input)))))
```
Тип результата: Tuple of (World, Struct)
Результат совместим только с записью в провайдер result.

{% cut "Пример полной программы" %}

``` lisp
(
(let mr_source (DataSource 'yamr 'betula))

(let x (Read! world mr_source
    (Key '('tablescheme (String 'alexnick_res)))
))

(let world (Left! x))
(let scheme (Right! x))

(let res_sink (DataSink 'result))
(let world (Write! world res_sink (Key) scheme '()))
(let world (Commit! world res_sink))
(return world)
)
```

{% endcut %}
{% cut "Пример выдачи" %}

```
[
    {
        "Write" = [
            {
                "Data" = {
                    "Cluster" = "cedar";
                    "Name" = "Input";
                    "IsEmpty" = "false";
                    "IsSorted" = "false";
                    "CanWrite" = "true";
                    "RecordsCount" = 5;
                    "DataSize" = 2345;
                    "ChunkCount" = 1;
                    "ModifyTime" = 0;
                    "Fields" = [
                        {
                            "Name" = "key";
                            "Type" = [
                                "DataType";
                                "String"
                            ];
                            "ClusterSortOrder" = [];
                            "Ascending" = []
                        };
                        {
                            "Name" = "subkey";
                            "Type" = [
                                "DataType";
                                "String"
                            ];
                            "ClusterSortOrder" = [];
                            "Ascending" = []
                        };
                        {
                            "Name" = "value";
                            "Type" = [
                                "DataType";
                                "String"
                            ];
                            "ClusterSortOrder" = [];
                            "Ascending" = []
                        }
                    ];
                    "MetaAttr" = {}
                }
            }
        ]
    }
]
```

{% endcut %}
### Чтение списка таблиц

Пример:
``` lisp
(let x (Read! world mr_source (Key '('tablelist (String 'Folder)))))
```
Тип результата: Tuple of (World, Struct)
Результат совместим только с записью в провайдер result.
Folder — имя папки для получения списка дочерних папок и таблиц. Для корневой папки необходимо задать пустую строку ` ('"")`.

{% cut "Пример полной программы" %}

``` lisp
(
(let mr_source (DataSource 'yamr 'betula))
(let x (Read! world mr_source (Key '('tablelist (String '"RootFolder")))))
(let world (Left! x))
(let tables (Right! x))
(let res_sink (DataSink 'result))
(let world (Write! world res_sink (Key) tables '()))
(let world (Commit! world res_sink))
(return world)
)
```

{% endcut %}

{% cut "Пример выдачи" %}

```
[
    {
        "Write" = [
            {
                "Data" = {
                    "Prefix" = "";
                    "Folders" = [
                        "Folder1"
                    ];
                    "Tables" = [
                        "File2"
                    ]
                }
            }
        ]
    }
]

```

{% endcut %}
### Конфигурирование провайдера

``` lisp
(let world (Configure! world mr_source options...))
```
Тип результата: World
Поддерживаемые опции:
'AllowEmptyInputTables — разрешить чтение из пустых таблиц.

### Запись ссылки результата вычисления в провайдер Result

Поддерживается использование режима ref при записи результатов чтения таблицы/чтения временной таблицы.
Если использовалось что-то кроме чтения исходной таблицы (например, обработка операцией Map), то сначала должен быть проведен коммит в соответствующем sink, а затем — в провайдере Result.

{% cut "Пример полной программы" %}

``` lisp
(
# read data from Input table
(let mr_source (DataSource 'yamr 'cedar))
(let x (Read! world mr_source
    (Key '('table (String 'Input)))
    '('key 'subkey 'value) '()))
(let world (Left! x))
(let table1 (Right! x))

# filter keys less than 100
(let tresh (Int32 '100))
(let table1low (FlatMap table1 (lambda '(item) (block '(
   (let intValueOpt (FromString (Member item 'key) 'Int32))
   (let ret (FlatMap intValueOpt (lambda '(item2) (block '(
      (return (ListIf (< item2 tresh) item))
   )))))
   (return ret)
)))))

# write table1low to result sink by reference
(let res_sink (DataSink 'result))
(let world (Write! world res_sink
    (Key)
    table1low '('('ref))))

# finish
(let mr_sink (DataSink 'yamr 'cedar))
(let world (Commit! world mr_sink))
(let world (Commit! world res_sink))
(return world)
)

```

{% endcut %}
{% cut "Пример выдачи для обычной таблицы" %}

```
[
    {
        "Write" = [
            {
                "Ref" = [{
                    "Reference" = [
                        "yamr";
                        "cedar";
                        "Input"
                    ];
                    "Remove" = "false"
                }]
            }
        ]
    }
]

```

{% endcut %}
{% cut "Пример выдачи для временной таблицы" %}

```
[
    {
        "Write" = [
            {
                "Ref" = [{
                    "Reference" = [
                        "yamr";
                        "cedar";
                        "tmp/bb686f68-2245bd5f-2318fa4e-1"
                    ];
                    "Remove" = "true"
                }]
            }
        ]
    }
]

```

{% endcut %}
{% endif %}

## Result

Провайдер для записи результатов выполнения.
Поддерживается в форме DataSink.
Формат:
(DataSink 'result)

Для данного провайдера заданы настройки:

* ограничение на общий объем результата в байтах;
* максимальный размер списков, если записывается список;
* тип YSON (binary/text/pretty).

### Запись

``` lisp
(let world (Write! world res_sink (Key) data '()))
```
Ограничение на тип данных — либо он должен быть чистым (не зависеть от world), либо он является результатом чтения/трансформации чтения некоторого DataSource.

Опции задаются в виде Tuple of (OptionTuple1, OptionTuple2...)
Формат OptionTuple: Tuple of (name:Atom [data...])

Поддерживаемые опции:
#### type — вывести тип данных в тег Type

Пример
``` lisp
(let world (Write! world res_sink (Key) data '('('type))))
```
#### take — дополнительное ограничение количества строк

Аргументы: value:Atom (integer)

Пример
``` lisp
(let world (Write! world res_sink (Key) data '('('take '5))))
```
Не имеет эффекта, если тип Data — не список.

#### ref — по возможности записать ссылку на данные вместо данных
Пример
``` lisp
(let world (Write! world res_sink (Key) data '('('ref))))
```
#### Формат выдачи
Результатом выдачи является последовательность YSON-данных следующего формата:

```
{
"Write"= [
результаты первой записи;
результаты второй записи
]
"Truncated":"true"
}
```
Поле Truncated появляется, только если превышены лимиты по объему выдачи. При этом могут быть отброшены некоторые записи.

Формат одной записи:
```
{
"Data" = значение
"Type" = тип значения
"Ref" = ссылка на значение
"Truncated":"true"
}
```
Поле Truncated появляется, только если тип Data — список и превышены лимиты по объему выдачи или длине списка.
Каждое из полей Data, Type или Ref не обязательно.
Формат поля Ref определяется провайдером, формирующим ссылку на значение.

{% cut "Пример форматирования типов данных" %}

<pre>
Void ->  ScalarString "Void"
Data(String) ->  ScalarString "value"
Data(Int32,Int64)  -> ScalarInt64 value
Data(Unt32,Uint64)  -> ScalarUint64 value
Data(Bool) -> ScalarBool value
List of (value1, value2) -> [value1;value2]
Tuple of (value1, value2) -> [value1;value2]
Just value -> [value]
Nothing T -> []
Struct of (name1: value, name2: value2) -> {"name1":value1;"name2":value2} или [value1;value2] когда имена/порядок полей определяются типом структуры.
Dict of (key1=>value1, key2=>value2) -> [[key1;value1];[key2;value2]]
Callable of (T1,T2)->R ->  ScalarString "Callable"
Variant of underlying type -> [index; item]

TypeOf(Void) -> ["VoidType"]
TypeOf(Data(String)) -> ["DataType";"String"]
TypeOf(List of T) -> ["ListType";TypeOf(T)]
TypeOf(Optional of T) -> ["OptionalType";TypeOf(T)]
TypeOf(Tuple of T1,T2,...) -> ["TupleType";[TypeOf(T1);TypeOf(T2)...]]
TypeOf(Struct of m1:T1,m2:T2,...) -> ["StructType";[[m1;TypeOf(T1)];[m2;TypeOf(T2)]...]]
TypeOf(Dict of K,V) -> ["DictType";TypeOf(K);TypeOf(V)]
TypeOf(Callable of (T1,T2)->R) -> ["CallableType";[optionalArgsCount;payload];[TypeOf(R)];[[TypeOf(T1);arg1name;arg1flags];[TypeOf(T2);arg2name;arg2flags]...]]
значения по умолчанию для optionalArgsCount = 0, payload — пустая строка, имен аргументов — пустая строка и флаги аргументов — 0.
TypeOf(Variant of underlying type) -> ["VariantType";TypeOf(T)]
</pre>

{% endcut %}
