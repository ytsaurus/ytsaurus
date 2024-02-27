# Форматы

В разделе описаны типы данных и форматы представления табличных данных, поддерживаемые системой {{product-name}}.

## Типы данных { #data_types }

В системе {{product-name}} хранятся данные следующих типов:

- [Структурированные данные](#structured_data)
- [Табличные данные](#table_data)
- [Бинарные данные](#binary_data)

Разные форматы позволяют работать с разными типами данных.

### Структурированные данные { #structured_data }

Структурированные данные — дерево узлов с атрибутами, в котором узлы соответствуют [YSON-типам](../../../user-guide/storage/yson.md) (map, list, простые типы) с атрибутами.
Структурированные данные можно получить при помощи команды [`get`](../../../api/commands.md#get).

Поддерживаемые форматы: `json`, `yson`.

Пример: узлы [Кипариса](../../../user-guide/storage/cypress.md), их атрибуты. 

### Табличные данные { #table_data }

Табличные данные — последовательность строк, в которой каждая строка логически представляет собой пару ключ-значение, где ключ — строка, значение — структурированные данные.

Поддерживаемые форматы: `json`, `yson`, `dsv`,  `schemaful_dsv`, `protobuf`, `arrow`.

Примеры: входные и выходные данные операций, входные данные команды [`write_table`](../../../api/commands.md#write_table), выходные данные команды [`read_table`](../../../api/commands.md#read_table).

### Бинарные данные { #binary_data }

Бинарные данные — последовательность байтов. Например, в файлах хранятся бинарные данные.

Поддерживаемые форматы: задать формат нельзя. Представление только в виде массива байтов.

Примеры: входные данные команды [`upload`](../../../api/commands.md#write_file) и выходные данные команды [`download`](../../../api/commands.md#read_file).

## Форматы представления табличных данных { #table_formats }

В {{product-name}} поддерживается несколько форматов данных, которые определяют, в каком виде данные будут переданы системе или получены от нее. 
Для всех команд [утилиты `yt`](../../../api/cli/cli.md), которые возвращают или принимают данные, необходимо указывать формат.

Чтение таблицы в формате DSV:

```bash
yt read //some/table --format dsv
```

{{product-name}} поддерживает следующие форматы представления данных:

* [YSON](#yson)
* [JSON](#json)
* [DSV (TSKV)](#dsv)
* [SCHEMAFUL_DSV](#schemaful_dsv)

{% if audience == "internal" %}
Для наглядности покажем, как будет выглядеть одна [таблица](https://yt.{{internal-domain}}/{{prestable-cluster}}/navigation?path=//home/tutorial/staff_unsorted_sample) в разных форматах.
{% endif %}

## Пример { #example }

В примерах будет использована данная таблица:

| name  | uid |
| :----------| :---------------: |
| Elena | 95792365232151958 |
| Denis | 78086244452810046 |
| Mikhail | 70609792906901286 |
| Ilya | 15696008603902587 |
| Oxana | 76840674253209974 |
| Alexey | 15943558469181404 |
| Roman | 37865805882228106 |
| Anna | 35039450424270744 |
| Nikolai | 45320538587295288 |
| Karina | 20364947097122776 |

## YSON { #yson }

Формат YSON является основным форматом {{product-name}}, в нем хранятся табличные данные, атрибуты узлов Кипариса и многие другие объекты. YSON является аналогом формата JSON, но включает понятие атрибутов узла и поддерживает бинарные данные. 
Подробная спецификация в разделе [YSON](../../../user-guide/storage/yson.md).

Для чтения таблицы в формате YSON выполните:

```bash
yt read --proxy {{prestable-cluster}} --format '<format=pretty>yson' '//home/tutorial/staff_unsorted_sample'
```

Представление таблицы в формате YSON:

```yson
{
    "name" = "Elena";
    "uid" = 95792365232151958;
};
{
    "name" = "Denis";
    "uid" = 78086244452810046;
};
{
    "name" = "Mikhail";
    "uid" = 70609792906901286;
};
{
    "name" = "Ilya";
    "uid" = 15696008603902587;
};
{
    "name" = "Oxana";
    "uid" = 76840674253209974;
};
{
    "name" = "Alexey";
    "uid" = 15943558469181404;
};
{
    "name" = "Roman";
    "uid" = 37865805882228106;
};
{
    "name" = "Anna";
    "uid" = 35039450424270744;
};
{
    "name" = "Nikolai";
    "uid" = 45320538587295288;
};
{
    "name" = "Karina";
    "uid" = 20364947097122776;
};
```

**Параметры**

В скобках указаны значения по умолчанию.
- **format** (`binary`) — описывает вид YSON, в котором будут сформированы данные. Указание вида имеет значение только при указании выходного по отношению к системе формата, для входного формата все разновидности YSON читаются системой гомогенно:
  - `binary` — бинарный,
  - `text` — текстовый, без форматирования,
  - `pretty` — текстовый, с форматированием.
- **skip_null_values** (`%false`) - включить пропуск значений `null` в схематизированных таблицах. По умолчанию все значения записываются.
- **enable_string_to_all_conversion** (`false`) — включить приведение строковых значений к численным и boolean типам. Например, `"42u"` преобразуется в `42u`, а `"false"` преобразуется в `%false`.
- **enable_all_to_string_conversion** (`false`) — включить приведение численных и boolean значений к строковому типу. Например, число `3.14` превратится в `"3.14"`, а `%false` в `"false"`.
- **enable_integral_type_conversion** (`true`) — включить приведение `uint64` к `int64` и наоборот. Обратите внимание, эта опция включена по умолчанию. Если при преобразовании происходит переполнение, то возникает соответствующая ошибка.
- **enable_integral_to_double_conversion** (`false`) — включить приведение `uint64` и `int64` к `double`. Например, целое число `42` превратится в дробное число `42.0`.
- **enable_type_conversion** (`false`) — включить все опции выше. В большинстве случаев достаточно пользоваться только этой опцией.
- **complex_type_mode** (`named`) — режим представления композитных типов, структур и вариантов,
  возможные значения `named` или `positional`, подробнее в [разделе](../../../user-guide/storage/data-types.md#yson).
- **string_keyed_dict_mode** (`positional`) - режим представления словей со строковыми ключами, возможные значения `named` или `positional`, подробнее в [специальном разделе](../../../user-guide/storage/data-types.md#yson).
- **decimal_mode** (`binary`) — режим представления типа `decimal`,
  возможные значения `text`, `binary`, подробнее в [разделе](../../../user-guide/storage/data-types.md#yson).
- **time_mode** (`binary`) — режим представления типов `date`, `datetime`, `timestamp`, возможные значения `text` или `binary`, подробнее в [разделе](../../../user-guide/storage/data-types.md#yson).
- **uuid_mode** (`binary`) — режим представления типа `uuid`, возможные значения `binary`, `text_yql` или `text_yt` подробнее в [разделе](../../../user-guide/storage/data-types.md#yson).

## JSON { #json }

[JSON](https://en.wikipedia.org/wiki/JSON) — широко распространенный формат для отображения структурированных данных.

Работать с JSON удобно, используя популярные инструменты, например [jq](https://stedolan.github.io/jq/). Кроме того, нет необходимости устанавливать дополнительные библиотеки для поддержки YSON.
Подробная спецификация [JSON](https://www.json.org/json-en.html).

Основным форматом {{product-name}} является YSON. В отличие от JSON он имеет атрибуты. Поэтому для преобразования данных из YSON в JSON необходимо закодировать атрибуты в JSON. 
Для этого конвертируйте узел с атрибутами в map-node c двумя ключами: 
- `\$value` — его значением становится весь текущий узел без атрибутов;
- `\$attributes` — его значением становится `map` атрибутов.

Например, `<attr=10>{x=y}` представляется в виде `{"$value": {"x": "y"}, "$attributes": {"attr": 10}}`.

Строки в {{product-name}} — YSON-строки — являются байтовыми, в то время как JSON использует кодирование Unicode. 
У формата есть настройка `encode_utf8`, позволяющая управлять конверсией. `encode_utf8` по умолчанию равен `%true`.

- encode_utf8=%true { #utf8-true }

Чтобы преобразовать YSON-строку в JSON-строку, нужно перевести каждый байт в unicode-символ с соответствующим номером и закодировать в UTF-8. Такое преобразование происходит, например, при чтении табличных данных командой [`read_table`](../../../api/commands.md#read_table).

При преобразовании JSON-строки в YSON-строку происходит проверка последовательности unicode-символов JSON-строки: каждый из них должен находиться в диапазоне от 0 до 255. Затем они конвертируются в соответствующие байты. Такое преобразование выполняется при записи табличных данных командой [`write_table`](../../../api/commands.md#write_table). 

{% note warning "Внимание" %}

При `encode_utf8=%true` unicode-символы вне диапазона 0..255 в JSON-строке не допускаются.

{% endnote %}

- encode_utf8=%false { #utf8-false }

Чтобы преобразовать YSON-строку в JSON-строку требуется, чтобы в YSON-строке находилась последовательность, допустимая UTF-8. Она будет преобразована в unicode-строку JSON.

При преобразовании JSON-строки в YSON-строку выполняется UTF8-кодирование unicode-символов в байтовые последовательности. Такое преобразование выполняется командой [`write_table`](../../../api/commands.md#write_table). 

{% note warning "Внимание" %}

В общем случае байтовые последовательности в таблицах не всегда являются UTF8-последовательностями. Поэтому при `encode_utf8=%false` не все YSON-строки доступны для чтения.

{% endnote %}

Запись UTF-8 строки:

{% list tabs %}
- Python

   ```python
   yt.write_table("//path/to/table", [{"key": "Иван"}], format=yt.JsonFormat(attributes={"encode_utf8": False}))
   ```

- CLI

   ```bash
   echo '{"key": "Иван"}{"key":"Иванов"}' | YT_PROXY={{production-cluster}} yt write --table "//path/to/table" --format="<encode_utf8=%false>json"
   ```
{% endlist %}

{% note info "Примечание" %}

При чтении пользователем таблицы в формате JSON каждая запись будет сделана в отдельной строке. При этом формат `pretty` не поддерживается.

{% endnote %}

Чтение таблицы в формате JSON:

```bash
yt read --proxy {{prestable-cluster}} --format json '//home/tutorial/staff_unsorted_sample'
```

Представление таблицы в формате JSON:

```json
{"name":"Elena","uid":95792365232151958}
{"name":"Denis","uid":78086244452810046}
{"name":"Mikhail","uid":70609792906901286}
{"name":"Ilya","uid":15696008603902587}
{"name":"Oxana","uid":76840674253209974}
{"name":"Alexey","uid":15943558469181404}
{"name":"Roman","uid":37865805882228106}
{"name":"Anna","uid":35039450424270744}
{"name":"Nikolai","uid":45320538587295288}
{"name":"Karina","uid":20364947097122776}
```

**Параметры**

В скобках указаны значения по умолчанию.
- **format** (`text`) — описывает формат JSON, в котором будут сформированы данные. Имеет значение только при указании выходного по отношению к системе формата. Для входного формата все разновидности JSON читаются системой гомогенно:
  - `text` — текстовый без форматирования;
  - `pretty` — текстовый с форматированием.
- **attributes_mode** (`on_demand`) — режим работы с атрибутами:
  - `always` — каждый узел превращается в map с `\$value` и `\$attributes`;
  - `never` — атрибуты игнорируются;
  -  `on_demand` — только узлы с непустыми атрибутами превращаются в map с `\$value` и `\$attributes`.
- **encode_utf8** (`true`) — включить интерпретацию UTF-8 символов в байты с соответствующими номерами.
- **string_length_limit** — ограничение на длину строки в байтах. При превышении лимита строка будет обрезана, а результат записан в виде `{$incomplete: true, $value:...}`.
- **stringify** (`false`) — включить преобразование всех скалярных типов в строки.
- **stringify_nan_and_infinity** (`false`) - включить преобразование `Nan` и `Infinity` в строки. Не может быть указан вместе с `support_infinity`.
- **support_infinity** (`false`) - разрешить значение `Nan` в типе `double`. Не может быть указан вместе с `stringify_nan_and_infinity`.
- **annotate_with_types** — включить аннотацию типов `{$type: "uint64", $value: 100500}`.
- **plain** — включить опцию парсера JSON, которая отключает учет логики про специальные ключи `\$attributes`, `\$value`, `\$type` и разбирает JSON как есть. При включенной опции парсер работает существенно быстрее.
- **enable_string_to_all_conversion** (`false`) — включить приведение строковых значений к численным и `boolean`-типам. Например, `"42u"` преобразуется в `42u`, а `"false"` преобразуется в `%false`.
- **enable_all_to_string_conversion** (`false`) — включить приведение численных и `boolean`-значений к строковому типу. Например, число `3.14` превратится в `"3.14"`, а `%false` в `"false"`.
- **enable_integral_type_conversion** (`true`) — включить приведение `uint64` к `int64` и наоборот. Если при преобразовании происходит переполнение, возникает соответствующая ошибка.
- **enable_integral_to_double_conversion** (`false`) — включить приведение `uint64` и `int64` к `double`. Например, целое число `42` превратится в дробное число `42.0`.
- **enable_type_conversion** (`false`) — включить все перечисленные опции. В большинстве случаев достаточно пользоваться только этой опцией.

{% note warning "Внимание" %}

JSON имеет некомпактное представление байтов с номерами 0-32 (а именно \u00XX). При попытке записать длинную строку из таких данных может возникнуть ошибка **Out of memory** в библиотеке Yajl, так как ограничение на объем памяти равно `2 * row_weight`. В таком случае не рекомендуется использовать формат JSON.

{% endnote %}

## DSV (TSKV) { #dsv }

**DSV** — Delimiter-Separated Values.
**TSKV** — Tab-Separated Key-Value.

Формат, широко используемый для хранения логов и работы с ними. DSV и TSKV — это два названия одного и того же формата.
Данный формат поддерживает только плоские записи с произвольным набором колонок и строковыми значениями. Записи разделены символом переноса строки `\n`. 
Поля в записи разделяются символом табуляции `\t`. 
Подробнее можно прочитать ниже.

Пример записи: `time=10\tday=monday\n`.

{% note warning "Внимание" %}

Формат не поддерживает атрибуты и типизированные значения. В примере `10` будет распознано системой как строка, а не как число.

{% endnote %}

Чтение таблицы в формате DSV:

```bash
yt read --proxy {{prestable-cluster}} --format dsv '//home/tutorial/staff_unsorted_sample'
```

Представление таблицы в формате DSV:

```
name=Elena      uid=95792365232151958
name=Denis	uid=78086244452810046
name=Mikhail	uid=70609792906901286
name=Ilya	uid=15696008603902587
name=Oxana	uid=76840674253209974
name=Alexey	uid=15943558469181404
name=Roman	uid=37865805882228106
name=Anna	uid=35039450424270744
name=Nikolai	uid=45320538587295288
name=Karina	uid=20364947097122776
```

Чтение таблицы с некоторыми параметрами:

```bash
yt read --proxy {{prestable-cluster}} --format '<field_separator=";";key_value_separator=":">dsv' '//home/tutorial/staff_unsorted_sample'
```

**Параметры:**

В скобках указаны значения по умолчанию.
- **record_separator** (`\n`) — разделитель записей.
- **key_value_separator** (`=`) — разделитель пары ключ-значение.
- **field_separator** (`\t`) — разделитель полей в записи.
- **line_prefix** (по умолчанию колонка отсутствует) — обязательная колонка в начале каждой записи.
- **enable_escaping** (`true`) — включить escaping записей.
- **escape_carriage_return** (`false`) — включить escaping символа `\r`.
- **escaping_symbol** (`\`) — символ экранирования.
- **enable_table_index** (`false`) – включить вывод индекса входной таблицы.
- **table_index_column**  (`@table_index`) – название колонки, в которой будет находиться индекс таблицы.
- **enable_string_to_all_conversion** (`false`) — включить приведение строковых значений к численным и boolean-типам. Например, `"42u"` преобразуется в `42u`, а `"false"` преобразуется в `%false`.
- **enable_all_to_string_conversion** (`false`) — включить приведение численных и boolean-значений к строковому типу. Например, число `3.14` преобразуется в `"3.14"`, а `%false` в `"false"`.
- **enable_integral_type_conversion** (`true`) — включить приведение `uint64` к `int64` и наоборот. Если при преобразовании происходит переполнение, возникает соответствующая ошибка.
- **enable_integral_to_double_conversion** (`false`) — включить приведение `uint64` и `int64` к `double`. Например, целое число `42` преобразуется в дробное число `42.0`.
- **enable_type_conversion** (`false`) — включить все перечисленные выше опции. В большинстве случаев достаточно пользоваться только этой опцией.

{% note info "Примечание" %}

Практически произвольная строка является валидной DSV-записью. Если после ключа нет key-value разделителя, поле игнорируется.

{% endnote %}

## SCHEMAFUL_DSV { #schemaful_dsv }

Формат является разновидностью DSV. При использовании SCHEMAFUL_DSV данные формируются в виде набора значений, разделенных табами. 
У формата есть обязательный атрибут `columns`, в котором через точку с запятой необходимо задать имена интересующих колонок. Если хотя бы в одной из записей отсутствует значение одной из колонок, возникнет ошибка (смотрите **missing_value_mode**).

Например, если в таблице есть две записи `{a=10;b=11} {c=100}` и указан формат `<columns=[a]>schemaful_dsv`, возникнет ошибка вида `Column "a" is in schema but missing`. 
Если в таблице будет только первая запись, на вход джобу пришла бы запись`10`.

{% note info "Примечание" %}

При работе с {{product-name}} CLI необходимо писать атрибуты в кавычках. Например: `--format="<columns=[a;b]>schemaful_dsv"`

{% endnote %}

Чтение таблицы в формате SCHEMAFUL_DSV:

```bash
yt read --proxy {{prestable-cluster}} --format '<columns=[name;uid]>schemaful_dsv' '//home/tutorial/staff_unsorted_sample'
```

Представление таблицы в формате SCHEMAFUL_DSV:

```schemaful_dsv
Elena   95792365232151958
Denis	78086244452810046
Mikhail	70609792906901286
Ilya	15696008603902587
Oxana	76840674253209974
Alexey	15943558469181404
Roman	37865805882228106
Anna	35039450424270744
Nikolai	45320538587295288
Karina	20364947097122776
```

**Параметры:**

В скобках указаны значения по умолчанию.
- **record_separator** (`\n`) — разделитель записей.
- **field_separator** (`\t`) — разделитель полей.
- **enable_table_index** (`false`) – включить печать переключения таблиц во входных данных.
- **enable_escaping** (`true`) – включает escaping служебных символов `\n`, `\t` и `\`.
- **escaping_symbol** (`\`) – символ экранирования.
- **columns** – список колонок, которые будут форматироваться, например `<columns=[columnA;columnB]>schemaful_dsv`.
- **missing_value_mode** (`fail`) — поведение при отсутствии значения – `NULL`:
  - `skip_row` — пропустить строку;
  - `fail` — прекратить выполнение операции, если указанной колонки нет в одной из строк;
  - `print_sentinel` — вместо отсутствующих значений задать `missing_value_sentinel`, который по умолчанию равен пустой строке.
- **missing_value_sentinel** (`пустая строка`) — значение отсутствующей колонки для режима **print_sentinel**.
- **enable_column_names_header** (`false`) — может быть использовано только при формировании данных в формате SCHEMAFUL_DSV, но не при разборе таких данных. Если значение равно `%true`, то в первой строке вместо значений будут имена колонок.
- **enable_string_to_all_conversion** (`false`) — включить приведение строковых значений к численным и boolean-типам. Например, `"42u"` преобразуется в `42u`, а `"false"` преобразуется в `%false`.
- **enable_all_to_string_conversion** (`false`) — включить приведение численных и boolean-значений к строковому типу. Например, число `3.14` превратится в `"3.14"`, а `%false` в `"false"`.
- **enable_integral_type_conversion** (`true`) — включить приведение `uint64` к `int64` и наоборот. Обратите внимание, эта опция включена по умолчанию. Если при преобразовании происходит переполнение, то возникает соответствующая ошибка.
- **enable_integral_to_double_conversion** (`false`) — включить приведение `uint64` и `int64` к `double`. Например, целое число `42` превратится в дробное число `42.0`.
- **enable_type_conversion** (`false`) — включить все перечисленные выше опции. В большинстве случаев достаточно пользоваться только этой опцией.

## ARROW { #Arrow }

[Arrow](https://arrow.apache.org/overview) (apache arrow) - бинарный формат, разработанный как универсальный стандарт для эффективного представления и обмена поколоночными данными в памяти между различными системами.

Преимущества формата:

- В отличие от строкового представления, где данные каждой строки хранятся вместе, arrow организует данные в столбцы. Это позволяет более эффективно выполнять некоторые аналитические запросы.

- Arrow поддерживает сложные типы данных, такие как структуры, списки, словари и другие.

### Arrow в {{product-name}}

При чтении таблицы в формате arrow возвращается конкатенация нескольких [IPC Streaming Format](https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format) потоков. 

```
<SCHEMA>
<DICTIONARY 0>
...
<DICTIONARY k - 1>
<RECORD BATCH 0>
...
<DICTIONARY x DELTA>
...
<DICTIONARY y DELTA>
...
<RECORD BATCH n - 1>
<EOS 0x00000000>
<SCHEMA>
...
<EOS 0x00000000>
...
<EOS 0x00000000>
```

 Возвращается несколько сконкатенированных потоков, а не один общий из-за того, что в {{product-name}} одна и та же колонка может храниться по-разному в разных чанках. Например, данные колонки могут храниться как явно целиком, так и в виде словаря (аналогично [словарю в arrow](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout)). При представлении данных в arrow формате мы сохраняем кодировку в виде словаря, и отдаем несколько сконкатенированных потоков, если какая-то колонка в одном чанке закодирована целиком, а в другом как словарь.

Ограничения и особенности:

- Чтение и запись можно производить только со схематизированными таблицами.

- Рекомендуется читать в arrow таблицы с поколоночным форматом хранения чанков `optimize_for = scan`, чтобы чтение было эффективно.

{% note warning "Внимание" %}

Запись в arrow пока не оптимизирована для таблиц с поколоночным форматом хранения чанков, происходит двойная трансформация данных, из поколоночного представления трансформируется в построчное и обратно.

{% endnote %}

### Типы

Для чтения таблиц сейчас поддержаны такие типы:

- `string` отображается в [arrow::binary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6BINARYE)
- `integer` отображается в какой-то из [arrow::integer](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5UINT8E) в зависимости от 
битности и знака
- `boolean` отображается в [arrow::bool](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type4BOOLE)
- `float` отображается в [arrow::float](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5FLOATE)
- `double` отображается в [arrow::double](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DOUBLEE)
- `date` отображается в [arrow::date32](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DATE32E)
- `datetime` отображается в [arrow::date64](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6DATE64E)
- `timestamp` отображается в [arrow::timestamp](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type9TIMESTAMPE)
- `interval` отображается в [arrow::int64](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type5INT64E)
- `cложные типы` представляются в виде [arrow::binary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6BINARYE), где хранится [yson представление](https://yt.yandex-team.ru/docs/user-guide/storage/data-types#yson) этих типов

На запись поддержаны те же типы, что и на чтение, а также дополнительно поддержаны:

- [arrow::list](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type4LISTE) представляется, как `list`
- [arrow::map](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type3MAPE) представляется, как `dict`
- [arrow::struct](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type6STRUCTE) представляется, как `struct`

Другие особенности работы с типами:

-  При чтении таблицы почти любой из типов может прийти, как [arrow::dictionary](https://arrow.apache.org/docs/cpp/api/datatype.html#_CPPv4N5arrow4Type4type10DICTIONARYE), где элементы словаря будут значениями, которые встречаются в колонке, а индексы указывают на порядок значений, подробнее [тут](https://arrow.apache.org/docs/format/Columnar.html#dictionary-encoded-layout).

- формат arrow поддерживает одну вложенность optional, более глубокая вложенность при чтении будет возвращена бинарным типом, так же как и для остальных сложных типов.

### Пример операций

Чтение таблицы в arrow:

{% list tabs %}
- Python

   ```python
   yt.read_table("//path/to/table", format=yt.format.ArrowFormat(), raw=True)
   ```

- CLI

   ```bash
    yt read --table "//path/to/table" --format arrow
   ```
{% endlist %}

Map операция с arrow:

```bash
yt map --input-format arrow --output-format arrow  --src "//path/to/input_table" "cat" --dst '<schema=[{name=item_1;type=int64};{name=item_2;type=string}]>//path/to/output_table'
```

### Операции с несколькими таблицами

Если на вход операции в формате arrow передать несколько таблиц, то внутри операции будет получен поток так же в виде конкатенации нескольких IPC Streaming Format потоков, где каждая часть может принадлежать одной из таблиц. Индекс таблицы передается в виде [метаданных схемы](https://arrow.apache.org/docs/format/Columnar.html#schema-message) с именем `TableId`, подробнее о метаданных в arrow можно почитать [тут](https://arrow.apache.org/docs/format/Columnar.html#custom-application-metadata)


## PROTOBUF { #PROTOBUF }

Protobuf - протокол передачи структурированных данных для работы с таблицами в С++ API.

Подробнее можно прочитать в разделе [Protobuf](../../../api/cpp/protobuf.md).
