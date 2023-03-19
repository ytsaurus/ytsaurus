# Protobuf-представление таблиц

В данном разделе описано использование протокола передачи структурированных данных [protobuf](https://en.wikipedia.org/wiki/Protocol_Buffers) для работы с таблицами в С++ API.

## Введение { #introduction }

С++ API позволяет использовать классы (сообщения) protobuf для чтения и записи таблиц как клиентом, так и внутри джоба.

Рекомендуется использовать версию [proto2](https://protobuf.dev/programming-guides/proto/). В случае использования [proto3](https://protobuf.dev/programming-guides/proto3/) могут возникнуть ошибки: например, при попытке записать `0` в поле `required=%true`.

### Схема работы

Пользователь описывает proto-структуру в файле `.proto`. Proto-структура может быть размечена различными [флагами](#flags). Флаги влияют на то, как {{product-name}} будет формировать или интерпретировать поля, указанные внутри сообщений.

## Примитивные типы { #primitive }

Для работы с [типом данных {{product-name}}](../../../user-guide/storage/data-types.md) из первой колонки можно использовать соответствующий protobuf-тип из второй колонки в таблице ниже.

| **{{product-name}}**                          | **Protobuf**                                 |
|---------------------------------|----------------------------------------------|
| `string`, `utf8`                | `string`, `bytes`                            |
| `int{8,16,32,64}`               | `int{32,64}`, `sint{32,64}`, `sfixed{32,64}` |
| `uint{8,16,32,64}`              | `uint{32,64}`, `fixed{32,64}`                |
| `double`                        | `double`, `float`                            |
| `bool`                          | `bool`                                       |
| `date`, `datetime`, `timestamp` | `uint{32,64}`, `fixed{32,64}`                |
| `interval`                      | `int{32,64}`, `sint{32,64}`, `sfixed{32,64}` |

Если диапазон целочисленного {{product-name}}-типа не соответсвует диапазону protobuf-типа, выполняется проверка в момент кодирования или декодирования.

## Свойства optional / required { #optional_required }

Свойства `optional` / `required` в  protobuf не обязаны соответствовать опциональности колонок в YT. Система всегда делает проверку в момент кодирования или декодирования protobuf-сообщений.

Например, если колонка `foo` в {{product-name}} имеет тип `int64` (`required=%true`), для её представления можно использовать поле:

```protobuf
  optional int64 foo = 42;
```
Ошибок не будет, пока все protobuf-сообщения, которые записываются в таблицу, имеют заполненное поле `foo`.

## Вложенные сообщения { #embedded }

Исторически все вложенные protobuf-структуры записываются в {{product-name}} в виде байтовой строки, хранящей [сериализованное](#serialization) представление вложенной структуры.

Вложенные сообщения достаточно эффективны, но не позволяют удобно представлять значения в веб-интерфейсе или работать с ними другими способами (без помощи protobuf).

Этот способ также можно указать явно, выставив специальный [флаг](#flags) в поле:

```
optional TEmbeddedMessage UrlRow_1 = 1 [(NYT.flags) = SERIALIZATION_PROTOBUF];
```

Можно указать альтернативный флаг, тогда {{product-name}} будет относить поле к типу `struct`:

```protobuf
optional TEmbeddedMessage ColumnName = 1 [(NYT.flags) = SERIALIZATION_YT];
```

Для использования вложенных сообщений требуется, чтобы:

- у таблицы была задана [схема](../../../user-guide/storage/static-schema.md);
- соответствующая колонка (в примере: `ColumnName`) имела {{product-name}}-тип [struct](../../../user-guide/storage/data-types.md#schema_struct);
- поля типа `struct` соответствовали полям вложенного сообщения (в примере: `TEmbeddedMessage`).

{% note warning %}

Флаг во вложенных сообщениях не наследуется по умолчанию. Если для поля с типом `T` выставлен флаг `SERIALIZATION_YT`, то для структур, вложенных в `T`, поведение по умолчанию все равно будет соответствовать флагу `SERIALIZATION_PROTOBUF`.

{% endnote %}

## Повторяющиеся поля { #repeated }

Чтобы работать с повторяющимися (repeated) полями, нужно явно указать флаг `SERIALIZATION_YT`:

```protobuf
repeated TType ListColumn = 1 [(NYT.flags) = SERIALIZATION_YT];
```

В YT такое поле будет иметь тип `list`. Для использования повторяющихся полей требуется, чтобы:

- у таблицы была задана [схема](../../../user-guide/storage/static-schema.md);
- соответствующая колонка (в примере: `ListColumn`) имела {{product-name}}-тип [list](../../../user-guide/storage/data-types.md#schema_list);
- элемент {{product-name}}-типа `list` соответствовал типу колонки protobuf (в примере: `TType`). Это может быть [примитивный](#primitive) тип или [вложенное сообщение](#embedded).


{% note warning %}

Флаг `SERIALIZATION_PROTOBUF` для повторяющихся полей не поддерживается.

{% endnote %}

## Поля oneof { #oneof }

По умолчанию поля внутри `oneof`-группы соответствуют {{product-name}}-типу [variant](../../../user-guide/storage/data-types.md#schema_variant). Например, сообщение ниже будет соответствовать структуре с полями `x` типа `int64` и `my_oneof` типа `variant<y: string, z: bool>`:

```protobuf
message TMessage {
    optional int64 x = 1;
    oneof my_oneof {
        string y = 2;
        bool z = 3;
    }
}
```

При выводе схемы с помощью `CreateTableSchema<T>()` будет выведен аналогичный тип.

Чтобы группы `oneof` соответствовали полям структуры, в которой они описаны, используйте флаг `(NYT.oneof_flags) = SEPARATE_FIELDS`:

```protobuf
message TMessage {
    optional int64 x = 1;
    oneof my_oneof {
        option (NYT.oneof_flags) = SEPARATE_FIELDS;
        string y = 2;
        bool z = 3;
    }
}
```
Этому сообщению будет соответствовать структура с опциональными полями `x`, `y` и `z`.

## Поля map { #map }

Существует 4 варианта отображения такого поля в столбец в таблице. См. пример ниже:

```protobuf
message TValue {
    optional int64 x = 1;
}

message TMessage {
    map<string, TValue> map_field = 1 [(NYT.flags) = SERIALIZATION_YT];
}
```

Поле `map_field` в зависимости от своих флагов может соответствовать:
 - списку структур с полями `key` типа ` string` и `value` типа `string`, в котором будет лежать сериализованный protobuf `TValue` (как будто у поля `value` выставлен флаг `SERIALIZATION_PROTOBUF`). По умолчанию в этом случае установлен флаг `MAP_AS_LIST_OF_STRUCTS_LEGACY`.
 - списку структур с полями `key` типа `string` и `value` типа `Struct<x: Int64>` (как будто у поля `value` выставлен флаг `SERIALIZATION_PROTOBUF`). По умолчанию в этом случае установлен флаг `MAP_AS_LIST_OF_STRUCTS`.
 - словарю `Dict<String, Struct<x: Int64>>>`: флаг `MAP_AS_DICT`.
 - опциональному словарю `Optional<Dict<String, Struct<x: Int64>>>>`: флаг `MAP_AS_OPTIONAL_DICT`.

## Флаги { #flags }

С помощью флагов можно настроить поведение protobuf. Для этого необходимо подключить библиотечный `.proto` файл.

```protobuf
import "yt/yt_proto/yt/formats/extension.proto";
```

Флаги могут соответствовать сообщениям, `oneof`-группам и полям сообщений.

Указывать флаги можно на уровне `.proto` файла, сообщения, `oneof`-группы и поля сообщения.


### SERIALIZATION_YT, SERIALIZATION_PROTOBUF { #serialization }

Поведение этих флагов описано [выше](#embedded).

По умолчанию там, где это актуально, подразумевается `SERIALIZATION_PROTOBUF`. Есть возможность поменять флаг для одного сообщения:

```protobuf
message TMyMessage
{
    option (NYT.default_field_flags) = SERIALIZATION_YT;
    ...
}

```

### OTHER_COLUMNS { #other_columns }

Флагом `OTHER_COLUMNS` можно пометить поле типа `bytes`. В это поле помещается YSON-мап, содержащий представления всех полей, которые не описываются другими полями этой protobuf-структуры.

```protobuf
message TMyMessage
{
    ...
    optional bytes OtherColumns = 1 [(NYT.flags) = OTHER_COLUMNS];
    ...
}
```

### ENUM_STRING / ENUM_INT { #enum_int }

Флагами `ENUM_STRING` / `ENUM_INT` можно помечать поля типа `enum`:

- Если поле помечено `ENUM_INT`, то оно будет сохраняться в колонку в виде целого числа.
- Если поле помечено `ENUM_STRING`, то оно будет сохраняться в колонку в виде строки.

По умолчанию подразумевается `ENUM_STRING`.

```protobuf
enum Color
{
    WHITE = 0;
    BLUE = 1;
    RED = -1;
}

...
optional Color ColorField = 1 [(NYT.flags) = ENUM_INT];
...
```

### ANY { #any }

Флагом `ANY` можно помечать поля типа `bytes`. Такие поля содержат YSON-представление колонки любого простого типа.
Например, для колонки типа `string` можно написать следующий код:

```protobuf
// message.proto
message TMyMessage
{
    ...
    optional bytes AnyColumn = 1 [(NYT.flags) = ANY];
    ...
}
```

```c++
// main.cpp
TNode node = "Linnaeus";
TMyMessage m;
m.SetAnyColumn(NodeToYsonString(node));
```
