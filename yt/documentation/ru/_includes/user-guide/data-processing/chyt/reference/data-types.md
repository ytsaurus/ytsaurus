# Типы данных

В ClickHouse доступно большое количество разнообразных [типов данных](https://clickhouse.com/docs/ru/sql-reference/data-types). В {{product-name}} используется своя система типов данных, сильно пересекающаяся с системой типов ClickHouse, но отдельные типы данных {{product-name}} могут отстутствовать в CH и наоборот. Полный список поддерживаемых в системе {{product-name}} типов доступен в разделе [типы данных {{product-name}}](../../../../user-guide/storage/data-types.md).

 ## Примитивные типы данных

Ниже в таблице приведено соответствие между __примитивными__ (не составными) типами ClickHouse и {{product-name}}.

| Тип {{product-name}}         | Тип ClickHouse  | Комментарий                                                  |
| -------------- | --------------- | ------------------------------------------------------------ |
| `intN, uintN`  | `IntN, UIntN`   | Целочисленные числовые колонки с точки ClickHouse выглядят как (U)Int соответствующей разрядности. |
| `boolean`      | `YtBoolean`     | В обычном ClickHouse не бывает Boolean'ов, они представляются как однобайтные беззнаковые числа (`UInt8`) со значениями 0 и 1. В CHYT поддержан дополнительный тип `YtBoolean`, который аналогичен типу `UInt8` в вычислениях в ClickHouse, но при записи в {{product-name}}-таблицу интерпретируется как `boolean`. |
| `string, utf8` | `String`        | Строки доступны как string                                   |
| `double`       | `Float64`       |                                                              |
| `date`         | `Date`          |
| `datetime`     | `DateTime`      |
| `any`          | `String`        | Any-колонки читаются движком ClickHouse как строковые колонки, содержащие данные в YSON. Смотрите также: [`chyt.composite.default_yson_format`](../reference/settings.md). |
| `T` с `required = %false` | `Nullable(T)` | Если колонка некоторого типа `T` не является `required`, то она видна как Nullable-колонка |

## Составные типы данных

Полезные ссылки:
- [Система типов {{product-name}}](../../../../../user-guide/storage/objects.md).

В {{product-name}} доступны составные типы данных: списки, словари, опциональные типы, структуры, кортежи. По умолчанию составные значения видны движком CH так же, как и тип `Any`, т.е. в виде YSON-строки.

Если установить настройку `chyt.composite.enable_conversion` в 1, CHYT начнёт преобразовывать составные типы (указанные в схеме, то есть, обладающие `type_v3` типом) в соответствующие составные типы ClickHouse.

Пример запроса:

```bash
# Запрос без включенного преобразования составных типов. Составные типы
# видны как строки в binary YSON.
yt --proxy <cluster_name> clickhouse execute 'select * from `//home/user/sample_table_composite`' --format Vertical
Row 1:
──────
list:   [foo;bar;]
dict:   [(бинарные нечитаемые символы)]
struct: [(бинарные нечитаемые символы)]

Row 2:
──────
list:   []
dict:   []
struct: [(бинарные нечитаемые символы)]

# Запрос со включённым преобразованием.
yt --proxy <cluster_name> clickhouse execute 'select * from `//home/user/sample_table_composite`' --format Vertical --setting chyt.composite.enable_conversion=1 --format Vertical
Row 1:
──────
list:   ['foo','bar']
dict:   [('key1',42),('key2',-17)]
struct: (3.14,'pi')

Row 2:
──────
list:   []
dict:   []
struct: (0,'')

```

К сожалению, в отношении составных типов данных системы типов ClickHouse и {{product-name}} довольно сильно разнятся. Перечислим ниже основные отличия.

### Optional-типы { #optional }

В системе типов Яндекса любой тип `T` может быть сделан опциональным путём окружения в `optional<T>`, например, возможны типы данных `optional<optional<int64>>`, `optional<list<string>>` и `optional<tuple<int64, string>>`.

В ClickHouse возможности по помещению значений в `Nullable` сильно ограничены. В частности, невозможно поместить в `Nullable`:
- тип, который уже является `Nullable`; 
- массив (`Array`);
- кортеж (`Tuple`). 

Таким образом, ни один из трёх примеров типов выше не имеет своего представления в системе типов ClickHouse.

Чтобы иметь возможность обрабатывать такие типы в CHYT, используется следующее преобразование.

Если тип `T` соответствует типу `T'` системе типов ClickHouse, а типа `Nullable(T')` в CH не существует, `optional<T>` отображается в `T'` с заменой null на дефолтное значение для `T'`. Примеры:

- `optional<optional<int64>>` отображается в `Nullable(Int64)`, причём два вида различных исходных `null` склеиваются в один.

- `optional<list<string>>` отображается в `Array(String)`, причём исходный `null` превращается в пустой массив `[]`.

- `optional<tuple<int64, string>>` отображается в `Tuple(Int64, String)`, причём исходный `null` превращается в кортеж из двух значений по умолчанию для Int64 и String, т.е. в кортеж `(0, '')`.

### Словари { #dict }

В системе типов Яндекса для любых двух типов `K` и `V` возможно составить тип `dict<K, V>`. В ClickHouse нет ничего, похожего на словари, поэтому `dict<K, V>` отображается в `Array(Tuple(K', V'))`.

смотрите пример запроса выше: в первом значении в колонке `dict` находится словарь `{'key1' -> 42, 'key2' -> -17}`.

### Кортежи { #tuple }

Кортежи системы типов Яндекса отображаются в кортежи ClickHouse тривиальным образом.

### Структуры { #structures }

Структуры из системы типов Яндекса можно было бы отображать в именованные кортежи (`Named Tuple`) CH. К сожалению, в ClickHouse есть проблемы с поддержкой именованных кортежей. Сейчас они поддерживаются только на внешнем уровне описания сложного типа (смотрите релевантный [issue](https://github.com/ClickHouse/ClickHouse/issues/15587)). Таким образом, погрузить `Named Tuple` внутрь, например, списка, не получится. Для упрощения реализации на текущий момент было принято решение отображать структуры в обычные кортежи.

Таким образом, со структурами можно работать как с кортежами, т.е. доставать из них элементы по индексу через `.1`, `.2`, и так далее, либо используя функцию `tupleElement`:

```bash
 yt --proxy <cluster_name> clickhouse execute 'select toTypeName(struct), struct.1, struct.2  from `//home/user/sample_table_composite`' --format Vertical --setting chyt.composite.enable_conversion=1
Row 1:
──────
toTypeName(struct):      Tuple(Float64, String)
tupleElement(struct, 1): 3.14
tupleElement(struct, 2): pi

Row 2:
──────
toTypeName(struct):      Tuple(Float64, String)
tupleElement(struct, 1): 0
tupleElement(struct, 2):

```

### Tagged типы { #tagged }

`Tagged` типы из общей системы типов Яндекса отображаются в соответствующие типы без тега. Информация о теге теряется.

### Variants, Named variants, Any { #var-any }

Варианты и именованные варианты никак не поддержаны, логика их отображения такая же, как если бы где-то внутри сложного типа встретилось значение типа `Any`. А именно, любое значение типа `Variant`, `Named Variant`, `Any` просто превращается в YSON-строку с представлением значения.

## Советы по работе с недостаточно схематизированными данными { #schema }

Ниже описано несколько приёмов, которые позволяют обходить некоторые проблемы с типами данных.

Многие таблицы содержат даты и времена в текстовом или числовом представлении (например, в формате ISO или Unix ts). Чтобы привести их к временн**о**му типу данных, можно воспользоваться стандартными [функциями ClickHouse для работы с датой и временем](https://clickhouse.com/docs/ru/sql-reference/functions/date-time-functions), [reinterpret](https://clickhouse.com/docs/ru/sql-reference/functions/type-conversion-functions/#reinterpretasdate), а также [CAST](https://clickhouse.com/docs/ru/sql-reference/functions/type-conversion-functions/#type_conversion_function-cast).


Для работы с композитными данными (массивами, словарями, структурами) в {{product-name}} исторически использовался тип данных `Any`. Несмотря на появление поддержки составных типов в системе типов {{product-name}}, многие старые данные по-прежнему остаются представленными в `Any`: движку ClickHouse они будут доступны в виде бинарного YSON.

В свою очередь, для движка ClickHouse представление `Any` в виде бинарного YSON почти ни о чём не говорит, так как поддержка YSON не встроена в ClickHouse. ClickHouse интерпретирует подобные данные просто в виде бинарной строки с каким-то пользовательским форматом. Поэтому слой совместимости ClickHouse и {{product-name}} предоставляет ряд вспомогательных функций, позволяющих адресоваться по YSON-структуре с помощью [языка YPath](../../../../../user-guide/storage/yson-docs.md). А также интерпретировать её узлы как примитивные типы {{product-name}} (`Int64`, `UInt64`, `Double`, `Boolean`), как строки (`String`), либо как списки примитивных типов {{product-name}} (`Array(Int64)`, `Array(UInt64)`, `Array(Double)`, `Array(Boolean)`) с приведением к соответствующему встроенному типу ClickHouse. Подробнее о данных функциях можно прочесть в разделе [Функции для работы с YSON](../../../../user-guide/data-processing/chyt/reference/functions.md#yson_functions).

{% note warning "Внимание" %}

Конвертация из представления {{product-name}} в представление ClickHouse неизбежно будет приводить к увеличению времени выполнения запроса.

{% endnote %}

