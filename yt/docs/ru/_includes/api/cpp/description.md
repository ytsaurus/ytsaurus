# Описание

Для использования возможностей системы {{product-name}} существует [C++ клиент]({{source-root}}/yt/cpp/mapreduce).

Посмотреть [Примеры использования](../../../api/cpp/examples.md).

{% if audience == "public" %}

{% note warning %}

C++ клиент предоставляется в текущем виде как есть, поэтому:
- интерфейс клиента может меняться без обратной совместимости;
- в текущей версии поддерживается только статическая линковка;
- напишите на info@ytsaurus.tech если возникнут вопросы по сборке клиента. 

{% endnote %}

{% endif %}

## Общая информация

- Весь код находится в пространстве имён `NYT`.
- Все необходимые пользователю интерфейсы находятся [по ссылке]({{source-root}}/yt/cpp/mapreduce/interface).
- Реализация всех интерфейсов доступна [по ссылке]({{source-root}}/yt/cpp/mapreduce/client), соответственно, библиотеки необходимо связывать с `interface`, а конечные цели сборки (программы) с `client`.
- `ISomethingPtr` обычно определены для интерфейсов как `TIntrusivePtr`.
- Все обязательные параметры присутствуют явно в сигнатурах соответствующих функций. Все опциональные параметры команд вынесены во вспомогательные структуры `TOptions`. Эти структуры имеют builder-like интерфейс, позволяющий задавать параметры в виде `TWhateverOptions().Parameter1(value1).Parameter2(value2)` для однострочной записи, если это удобно.

## Логирование { #logging }

Логирование включается переменной окружения `YT_LOG_LEVEL`, которая может принимать одно из следующих значений: `ERROR`, `WARNING` (для совместимости с [Python API](../../../api/python/userdoc.md), эквивалентен `ERROR`), `INFO`, `DEBUG`.
Кроме того, в начале программы должна быть вызвана функция `NYT::Initialize()` (см. следующий раздел).

По возможности, рекомендуется сохранять логи на уровне `DEBUG`, особенно для production процессов. Такие логи многократно упростят и ускорят исправление проблем с API, если они возникнут. Эти же логи рекомендуется прикладывать к письмам о проблемах использования API.

## Инициализация { #init }

Перед началом клиентских действий необходимо вызвать метод `Initialize`.

В этом месте, во-первых, происходит ветвление исполнения между клиентским кодом и кодом джоба, а во-вторых, выполняется дополнительная системная инициализация (логирование и прочее).

Если программа не будет запускать операции, то можно вызвать `JoblessInitialize()`. Эта функция осуществит базовую инициализацию библиотеки (логирование, и т. д.).

Точкой входа на клиенте является функция `CreateClient(serverName)`, возвращающая указатель на интерфейс `IClient`.

Пример минимальной программы (файл `main.cpp`):

```c++
#include <yt/cpp/mapreduce/interface/client.h>
int main()
{
    NYT::Initialize();
    auto client = NYT::CreateClient("cluster_name");
    client->Create("//tmp/table", NYT::NT_TABLE);
}
```

Соответствующий ей файл `ya.make`:

```
PROGRAM()

PEERDIR(yt/cpp/mapreduce/client)

SRCS(main.cpp)

END()
```
{% if audience == "internal" %}
Если нужно в рамках одной программы иметь клиенты, работающие под разными [токенами авторизации](../../../user-guide/storage/auth.md), можно воспользоваться опциями:

```
auto client = CreateClient(serverName, TCreateClientOptions().Token(userToken));
```
{% endif %}

## Транзакции { #transactions }

Существуют два интерфейса (`IClient` и `ITransaction`), имеющие почти одинаковый набор методов, которые наследуются от `IClientBase`. В первом случае соответствующие команды исполняются без какой-либо клиентской транзакции, во втором - исполняются под транзакцией указанного класса. Для получения экземпляра `ITransaction` используется метод `IClientBase::StartTransaction()`. При использовании переменной окружения `YT_TRANSACTION` даже `IClient` начинает работать из-под родительской транзакции.

Пример работы с транзакциями:

```c++
#include <yt/cpp/mapreduce/interface/client.h>
int main()
{
    NYT::Initialize();
    auto client = NYT::CreateClient("cluster_name");
    auto tx = client->StartTransaction();
    tx->Remove("//tmp/table");
    tx->Commit();
}
```

У транзакции есть явные методы `Commit()` и `Abort()`. Если транзакция уничтожается без явного вызова одного из этих методов, в деструкторе будет предпринята попытка вызвать `Abort()`, но без гарантий, что метод действительно выполнится.

Транзакции могут быть иерархическими:

```c++
auto tx = client->StartTransaction();
auto innerTx = tx->StartTransaction();
innerTx->Create("//tmp/double", NT_DOUBLE);
innerTx->Commit();
tx->Commit();
```

Метод `ITransaction::GetId()` возвращает `TGUID` — идентификатор данной транзакции.

Транзакция, создаваемая через `StartTransaction()`, автоматически пингуется и управляется C++ клиентом.

Существует способ работать из-под транзакции, созданной извне. Для этого необходимо вызвать метод `IClient::AttachTransaction(transactionId)`. Все команды, вызываемые в таком объекте, будут выполняться в контексте транзакции с этим идентификатором, но пинговаться клиентом эта транзакция не будет.

Более подробно об устройстве транзакций можно почитать в разделе [Транзакции](../../../user-guide/storage/transactions.md).

## TNode { #tnode }

[TNode]({{source-root}}/yt/cpp/mapreduce/interface/node.h)  — это основной класс, обеспечивающий динамическое DOM-представление [YSON-документа](../../../user-guide/storage/yson-docs.md). Используется для работы с Кипарисом, передачи дополнительных спецификаций операций, как один из способов кодирования строк таблицы и т. д.

Примеры использования:

```c++
TNode i = 1; // all signed types -> int64
TNode u = 8u; // all unsigned types -> uint64
TNode b = true; // boolean
TNode d = 2.5; // double
TNode s = "foo"; // string

TNode l = TNode::CreateList();
l.Add(5);
l.Add(false);
Cout << l[1].AsBool() << Endl;

TNode m = TNode::CreateMap();
m["abc"] = 255u;
m("foo", "bar");
Cout << m["foo"].AsString() << Endl;

TNode e = TNode::CreateEntity();
if (e.IsEntity()) {
    Cout << "entity!" << Endl;
}
```

- Значения элементарных типов необходимо извлекать методами `TNode::AsType()`.
- Проверки типов можно производить методами `TNode::IsType()`;

Конструктор по умолчанию создаёт `TNode` с типом `TNode::Undefined`: такой объект не может быть сериализован в YSON без явной инициализации.

Для сокращения записи методы вставки в map и list сразу переводят `TNode` в нужный тип:

```c++
auto mapNode = TNode()("key1", "value1")("key2", "value2");
auto listNode = TNode().Add(100).Add(500);
```

## Работа с Кипарисом { #cypress }

Подробнее о командах для работы см. в разделе [Работа с деревом метаинформации](../../../user-guide/storage/cypress-example.md). Реализовано в виде интерфейса `ICypressClient`, от которого `IClientBase` наследуется.

Примеры использования:

```c++
TString node("//tmp/node");

client->Create(node, NYT::NT_STRING);

Cout << client->Exists(node) << Endl;

client->Set(node, "foo");
Cout << client->Get(node).AsString() << Endl;

TString otherNode("//tmp/other_node");
client->Copy(node, otherNode);
client->Remove(otherNode);

client->Link(node, otherNode);
```

Пример работы с атрибутами таблиц:

```c++
client->Set("//tmp/table/@user_attr", 25);

Cout << client->Get("//tmp/table/@row_count").AsInt64() << Endl;
Cout << client->Get("//tmp/table/@sorted").AsBool() << Endl;
```

### Create

[Создать](../../../api/commands.md#create) узел. [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#create).

```c++
TNodeId Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options);
```


| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         |  Путь до узла                      |
| `type`        | `ENodeType`  | -          | [Тип](../../../user-guide/storage/objects.md#object_types) создаваемого узла: <br/>  `NT_STRING` — строка (`string_node`);<br/> `NT_INT64` — целое знаковое число (`int64_node`); <br/>`NT_UINT64` — целое беззнаковое число (`uint64_node`); <br/>`NT_DOUBLE` — вещественное число (`double_node`); <br/>`NT_BOOLEAN` — булево значение (`boolean_node`); <br/>`NT_MAP` — словарь в Кипарисе (ключи — строки, значения — другие узлы, `map_node`); <br/>`NT_LIST` — упорядоченный список в (значения — другие узлы) (`list_node`);<br/> `NT_FILE` — [файл](../../../user-guide/storage/objects.md#files) (`file`); <br/>`NT_TABLE` — [таблица](../../../user-guide/storage/objects.md#tables) (`table`);<br/> `NT_DOCUMENT` — [документ](../../../user-guide/storage/objects.md#yson_doc) (`document`); <br/>`NT_REPLICATED_TABLE` — [реплицированная таблица](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md) (`replicated_table`); <br/>`NT_TABLE_REPLICA` — (`table_replica`); |
| `options` &mdash; необязательные опции: | | |                                  |
| `Recursive`   | `bool`       | `false`     | Создавать ли промежуточные директории. |
| `IgnoreExisting` | `bool`    | `false`     | Если узел существует, не делать ничего и не выдавать ошибку. |
| `Force`       | `bool`       | `false`     | Если узел существует, пересоздать его вместо ошибки. |
| `Attributes`  | `TNode`      | none        | [Атрибуты](../../../user-guide/storage/attributes.md) создаваемого узла. |

### Remove

[Удалить](../../../api/commands.md#remove) узел. [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#remove).

```c++
void Remove(
    const TYPath& path,
    const TRemoveOptions& options);
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         | Путь до узла.                      |
| `options` &mdash; необязательные опции: |  | |                                |
| `Recursive`   | `bool`       | `false`    | Рекурсивно удалить детей составного узла. |
| `Force`       | `bool`       | `false`    | Не прекращать работу, если указанного узла уже нет. |

### Exists

[Проверить существование](../../../api/commands.md#exists) объекта по указанному пути.

```c++
bool Exists(
    const TYPath& path)
```


| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         | Путь до узла.                      |
| `options` &mdash; необязательные опции |  | |                                |

### Get

[Получить содержимое узла Кипариса](../../../api/commands.md#get) в формате  `TNode`. [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#get).

```c++
TNode Get(
    const TYPath& path,
    const TGetOptions& options);
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         | Путь до узла или атрибута.         |
| `options` &mdash; необязательные опции: |  | |                                |
| `AttributeFilter` | `TMaybe<TAttributeFilter>` | none | Список атрибутов, которые нужно получить. |
| `MaxSize`     | `TMaybe<i64>`| none       | Ограничение на количество детей (для составных узлов). |

### Set

[Записать новое содержимое узла Кипариса](../../../api/commands.md#set). [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#set).

```c++
void Set(
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options);
```


| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         |  Путь до узла или атрибута.         |
| `value`       | `TNode`      |  -         | Записываемое поддерево (содержимое).            |
| `options` &mdash; необязательные опции: |  | |                                |
| `Recursive`   | `bool`       | `false`    | Создавать ли промежуточные узлы. |

### List

[Получить список потомков узла](../../../api/commands.md#list). [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#list).

```c++
TNode::TListType List(
    const TYPath& path,
    const TListOptions& options);
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `path`        | `TYPath` |  -         | Путь до узла или атрибута.         |
| `options` &mdash; необязательные опции: |  | |                                |
| `AttributeFilter` | `TMaybe<TAttributeFilter>` | none | Список атрибутов, которые нужно получить с каждым узлом. |
| `MaxSize`     | `TMaybe<i64>`| none       | Ограничение на количество детей (для составных узлов).   |

### Copy

[Скопировать узел Кипариса по новому адресу](../../../api/commands.md#copy). [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#copy_move).

{% note info "Примечание" %}

Копирование всегда рекурсивно, параметр `Recursive` позволяет задать, нужно ли создавать промежуточные директории для `destinationPath`.

{% endnote %}


```c++
TNodeId Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options);
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `sourcePath`  | `TYPath` | -          | Путь до исходного узла.             |
| `destinationPath` | `TYPath` | -      | Путь для создания копии (без опции `Force` — не должен существовать). |
| `options` &mdash; необязательные опции: | | |                                  |
| `Recursive`   |  `bool`      | `false`    | Создавать ли промежуточные директории для `destinationPath`. |
| `Force`       | `bool`       | `false`    | Если `destinationPath` существует, заменить на новое содержимое. |
| `PreserveAccount`| `bool`    | `false`    | Сохранять ли аккаунты исходных узлов. |
| `PreserveExpirationTime` | `bool` | `false` | Копировать ли атрибут `expiration_time`. |
| `PreserveExpirationTimeout` | `bool` | `false` | Копировать ли атрибут `expiration_timeout`. |

### Move

[Перенести узел по новому адресу](../../../api/commands.md#move). [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#copy_move).

```c++
TNodeId Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options);
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `sourcePath`  | `TYPath` | -          | Путь до исходного узла.             |
| `destinationPath` | `TYPath` | -      | Путь для создания копии (без опции `Force` — не должен существовать). |
| `options` &mdash; необязательные опции: | | |                                  |
| `Recursive`   |  `bool`      | `false`    | Создавать ли промежуточные директории для `destinationPath`. |
| `Force`       | `bool`       | `false`    | Если `destinationPath` существует, заменить на новое содержимое. |
| `PreserveAccount`| `bool`    | `false`    | Сохранять ли аккаунты исходных узлов. |
| `PreserveExpirationTime` | `bool` | `false` | Копировать ли атрибут `expiration_time`. |
| `PreserveExpirationTimeout` | `bool` | `false` | Копировать ли атрибут `expiration_timeout`. |

### Link

[Создать](../../../api/commands.md#link) [символическую ссылку](../../../user-guide/storage/links.md) на объект по новому адресу. [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#link).

```c++
TNodeId Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
```

| **Параметр**      | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| --------------|--------------|------------|---------------------------------- |
| `targetPath`  | `TYPath` | -          |Путь до исходного узла, на который будет идти ссылка.              |
| `linkPath`    | `TYPath` | -          |Путь, по которому будет создана ссылка (без опции `Force` — не должен существовать). |
| `options` &mdash; необязательные опции | | |                                  |
| `Recursive`   | `bool`       | `false`    | Создавать ли промежуточные директории.|
| `IgnoreExisting`| `bool`     | `false`    | Если `linkPath` существует и  *является ссылкой*, не делать ничего и не показывать ошибку |
| `Force`       | `bool`       | `false`    | Если узел `linkPath` существует, на его месте создать ссылку заново. |
| `Attributes`  | `TNode`      | none       | [Атрибуты](../../../user-guide/storage/attributes.md) для `linkPath` виде `TNode`, записываются в случае создания. |

### Concatenate

[Склеить](../../../api/commands.md#concatenate) набор файлов или таблиц (в том порядке, в котором указаны пути). [См. аналог в CLI](../../../user-guide/storage/cypress-example.md#concatenate).

Слияние данных происходит на уровне метаинформации исключительно на мастер-сервере [Кипариса](../../../user-guide/storage/cypress.md) .

```c++
void Concatenate(
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options);
```

| **Параметр**                   | **Тип**          | **Значение по умолчанию**    | **Описание**                          |
| ---------------------------|--------------|------------|---------------------------------- |
| `sourcePaths`              | `TVector<TYPath>`|  -    | Пути до входных файлов или таблиц. |
| `destinationPath`          | `TYPath`    | -          | Путь до объединённого файла или таблицы, должен существовать. |
| `options` &mdash; необязательные опции:|  |            |                                   |
|  `Append`                  | `bool`       | `false`    | Сохранить ли содержимое `destinationPath`, новые данные добавить в конец. Без этого флага старое содержимое `destinationPath` пропадёт. |

## Чтение/запись файлов

Для записи файла в YT необходимо вызвать метод клиента/транзакции `CreateFileWriter(filePath)`.
Возвращаемый интерфейс `IFileWriter` является наследником `IOutputStream`. Если файл ранее не существовал, он будет создан.

```c++
auto writer = client->CreateFileWriter("//tmp/file");
*writer << "something";
writer->Finish();
```

Для чтения используется интерфейс `IFileReader`, который, предоставляет методы `IInputStream`.

```c++
auto reader = client->CreateFileReader("//tmp/file");
TString anything;
*reader >> anything;
```

Дополнительные опции пробрасываются через структуры `TFileWriterOptions` и `TFileReaderOptions`.

## Структуры для представления строк таблиц

Существует несколько способов представления отдельных записей таблицы в операциях и во время ввода/вывода.

### TNode

Для представления записи TNode должен иметь тип [map](../../../user-guide/storage/objects.md#primitive_types) и представлять собой отображение имён колонок в YSON-значения.

{% if audience == "internal" %}

### TYaMRRow

Для работы в совместимом с YaMR формате:

```c++
struct TYaMRRow
{
    TStringBuf Key;
    TStringBuf SubKey;
    TStringBuf Value;
};
```
{% else %}{% endif %}

### Protobuf

Подробнее о Protobuf см. [Protobuf-представление таблиц](../../../api/cpp/protobuf.md).

В этом случае предоставляется возможность определить пользовательский protobuf тип. Для отображения protobuf полей в имена колонок используются расширения:

```c++
import "yt/yt_proto/yt/formats/extension.proto";
message TSampleProto {
    optional int64  a = 1 [(NYT.column_name) = "column_a"];
    optional double b = 2 [(NYT.column_name) = "column_b"];
    optional string c = 3 [(NYT.column_name) = "column_c"];
}
```

Есть поддержка вложенных структур, в этом случае используется бинарная протобуфная сериализация, т.е. в колонке таблицы будет храниться строка с бинарной protobuf сериализацией [вложенного сообщения](../../../api/cpp/protobuf.md#embedded). Во вложенных структурах могут быть `repeated` поля, они будут сериализованы бинарно вместе с остальной частью вложенного сообщения.

{% note warning %}

Не следует использовать версию `proto3`. Ввиду [особенностей реализации proto3](https://developers.google.com/protocol-buffers/docs/proto3#default), значения полей по умолчанию неотличимы от отсутствующих полей. Это приводит, например, к тому, что значение `0` типа `int` не будет записано в таблицу.

{% endnote %}


## Чтение и запись таблиц

См. [yt/cpp/mapreduce/interface/io.h]({{source-root}}/yt/cpp/mapreduce/interface/io.h).


```c++
auto writer = client->CreateTableWriter<TNode>("//tmp/table");
writer->AddRow(TNode()("x", 1.)("y", 0.));
writer->AddRow(TNode()("x", 0.)("y", 1.));
writer->Finish();
```

Запись блочная. При вызове `AddRow()` данные сохраняются во внутреннем буфере, и, если размер этого буфера превысит 64 MB, запускается отдельный тред, посылающий накопленные данные.

Метод `Finish()` гарантированно вызовет сброс всех накопленных записей или выдаст ошибку. При уничтожении writer без вызова `Finish()` в деструкторе будет произведена попытка сброса без гарантии результата. Если таблица не существовала до создания writer, она будет создана в контексте того же клиента или транзакции со всеми атрибутами по умолчанию. Если необходим другой коэффициент репликации, следует создать таблицу и добавить ей необходимые атрибуты заранее.

При чтении основными методами являются `GetRow()`, `IsValid()`, `Next()`:

```c++
auto reader = client->CreateTableReader<TYaMRRow>("//tmp/ksv_table");
for (; reader->IsValid(); reader->Next()) {
    auto& row = reader->GetRow();
    Cout << row.Key << "; " << row.SubKey << "; " << row.Value << Endl;
}
```

Возвращаемый из `GetRow()` объект можно использовать только до вызова `Next()`.

Метод `GetRowIndex()` позволяет получить абсолютный индекс текущей записи.

Пример с protobuf:

```c++
auto writer = client->CreateTableWriter<TSampleProto>("//tmp/table");
TSampleProto row;
row.set_a(42);
writer->AddRow(row);
```

### TRichYPath { #trichypath }

Структура `TRichYPath` позволяет передавать вместе с путём к таблице различные флаги. Это применимо как к функциям чтения и записи, так и к входным и выходным таблицам [операций](#operations).

Если необходима запись в режиме append:

```c++
client->CreateTableWriter(TRichYPath("//tmp/table").Append(true));
```

Если необходимо, чтобы результат записи был сортированным, используется атрибут `SortedBy`:

```c++
auto path = TRichYPath("//tmp/table").SortedBy({"a", "b"});
```

По protobuf-сообщению можно создать схему таблицы:

```c++
auto path = WithSchema("//tmp/table");
```

При наличии схемы указывать сортировку с помощью `SortedBy` уже нельзя. Можно указать сортировку при создании схемы:

```c++
auto path = WithSchema("//tmp/table", {"a", "b"});
```

Или отсортировать схему позже: `path.Schema_.SortBy({"a", "b"});`

Кроме этого, при чтении `TRichYPath` предоставляет возможность задавать горизонтальные и вертикальные выборки:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .LowerLimit(TReadLimit().RowIndex(10))
    .UpperLimit(TReadLimit().RowIndex(20)));
```

Синонимично можно написать:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange::FromRowIndices(10, 20));
```

Если таблица отсортирована, можно задавать диапазон по ключам. Для этого используется класс `TKey`, который представляет собой последовательность значений ключевых колонок или их префикс. В примере ниже подразумевается, что у таблицы `"//tmp/table"` есть две ключевые колонки типа `string` и `int64` (либо больше двух, в таком случае также работает лексикографическое сравнение).

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .LowerLimit(TReadLimit().Key({"foo", 100}))
    .UpperLimit(TReadLimit().Key({"foo", 200})));
```

Если необходимы строка (строки) с заданным ключом или индексом, можно использовать `TReadRange::Exact`:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .Exact(TReadLimit().Key({"foo", 100})));
```

Вертикальная выборка:

```c++
auto path = TRichYPath("//tmp/table").Columns({"a", "b"});
```

Переименование колонок:

```c++
auto path = TRichYPath("//tmp/table").RenameColumns({{"a", "b"}, {"c", "d"}});
```

Колонка таблицы *a* будет видна под именем *b*, а *c* &mdash; под именем *d*. Функция переустанавливает полный список переименованных колонок, т.е. повторный вызов `RenameColumns` сотрет результаты предыдущего. `Columns` применяется после `RenameColumns`, т.е. в данном случае *b* и *d* будут пригодны, *a* и *c* &mdash; не пригодны для `Columns`.

Также остаётся возможность использовать ненормализованные пути, например:

```c++
auto path = TRichYPath("//tmp/table[#0:#100500]");
```

### Настройки форматов

Для более тонкой настройки форматов существует класс [TFormatHints]({{source-root}}/yt/cpp/mapreduce/interface/client_method_options.h). Его можно передавать в полях `TTableReaderOptions::FormatHints` и `TTableWriterOptions::FormatHints` при создании reader/writer.
Если данная настройка имеет смысл для запрошенного формата, то она будет применена, иначе возникнет соответствующее исключение.

Доступные настройки:

- `SkipNullValuesForTNode` — для полей со значением `#`  не создавать ключи в хеш-мапе.
- [Преобразования типов](../../../user-guide/storage/data-types.md) при записи: `EnableStringToAllConversion`, `EnableAllToStringConversion`, `EnableIntegralTypeConversion%` (`uint64` <-> `int64`, включена по умолчанию), `EnableIntegralToDoubleConversion`, `EnableTypeConversion` (все указанные выше одновременно).

### Дополнительные опции

Если необходима ещё более тонкая настройка параметров чтения/записи, можно использовать структуры `TTableWriterOptions` и `TTableReaderOptions`. В них присутствует поле `Config`, в него в формате `TNode` можно передавать дополнительные настройки, соответствующие параметрам [table_writer](../../../user-guide/storage/io-configuration.md#table_writer) и [table_reader](../../../user-guide/storage/io-configuration.md#table_reader).

```c++
auto writer = client->CreateTableWriter<TNode>("//tmp/table",
    TTableWriterOptions().Config(TNode()("max_row_weight", 128 << 20)));
```

### Параллельное чтение { #parallel_read }

Для параллельного чтения таблиц есть [библиотека]({{source-root}}/yt/cpp/mapreduce/library/parallel_io/parallel_reader.h).

## Запуск операций { #operations }

См. [по ссылке]({{source-root}}/yt/cpp/mapreduce/interface/operation.h).

Чтобы запустить операцию, содержащую пользовательский код, необходимо:

- унаследоваться от одного из интерфейсов `IMapper`, `IReducer`;
- определить функцию `Do()`, в которой должен содержаться обработчик записей;
- использовать один из макросов `REGISTER_*`;
- вызвать в клиентском коде метод клиента/транзакции, соответствующий типу операции.

Интерфейсы `IMapper` и `IReducer` принимают в качестве шаблонных параметров типы `TTableReader<тип входных записей>` и `TTableWriter<тип выходных записей>`. Указатели на эти типы принимает функция `Do()`.

```c++
#include <yt/cpp/mapreduce/interface/operation.h>
class TExtractKeyMapper
    : public IMapper<TTableReader<TYaMRRow>, TTableWriter<TNode>>
{
public:
    void Do(TTableReader<TYaMRRow>* input, TTableWriter<TNode>* output) override {
        for (; input->IsValid(); input->Next()) {
            output->AddRow(TNode()("key", input->GetRow().Key));
        }
    }
};
```

Далее необходимо создать макрос, выполняющий регистрацию пользовательского класса:

```c++
REGISTER_MAPPER(TExtractKeyMapper);
```

Отсутствует необходимость в генерации уникальных id, для идентификации будет использован механизм type_info. Тип джоба будет явно виден в веб-интерфейсе операции в строке запуска пользовательских джобов:

```c++
./cppbinary --yt-map "TExtractKeyMapper" 1 0
```

Если пользователь желает идентифицировать свои джобы другим способом, он должен позаботиться об уникальности имён в рамках одного приложения и использовать макросы `REGISTER_NAMED_*`:

```c++
REGISTER_NAMED_MAPPER("The best extract key mapper in the world", TExtractKeyMapper);
```

Функции для запуска отдельных видов операций приведены ниже:

Map:

- ```c++
  IOperationPtr Map(
      const TMapOperationSpec& spec,
      ::TIntrusivePtr<IMapperBase> mapper,
      const TOperationOptions& options = TOperationOptions())
  ```

Reduce:

- ```c++
  IOperationPtr Reduce(
      const TReduceOperationSpec& spec,
      ::TIntrusivePtr<IReducerBase> reducer,
      const TOperationOptions& options = TOperationOptions())
  ```

Join-Reduce:

- ```c++
  IOperationPtr JoinReduce(
      const TJoinReduceOperationSpec& spec,
      ::TIntrusivePtr<IReducerBase> reducer,
      const TOperationOptions& options = TOperationOptions())
  ```

 MapReduce:

- ```c++
  IOperationPtr MapReduce(
      const TMapReduceOperationSpec& spec,
      ::TIntrusivePtr<IMapperBase> mapper,
      ::TIntrusivePtr<IReducerBase> reducer,
      const TOperationOptions& options = TOperationOptions())
  ```


  ```c++
  IOperationPtr MapReduce(
      const TMapReduceOperationSpec& spec,
      ::TIntrusivePtr<IMapperBase> mapper,
      ::TIntrusivePtr<IReducerBase> reduceCombiner,
      ::TIntrusivePtr<IReducerBase> reducer,
      const TOperationOptions& options = TOperationOptions())
  ```

В качестве `mapper` можно указать нулевой указатель, тогда операция запустится без стадии Map, вторая версия функции дополнительно запустит стадию [reduce_combiner](../../../user-guide/data-processing/operations/reduce.md#reduce_combiner).

В параметры вызова запуска операции обычно входят:

- структура с необходимыми параметрами спецификации;
- указатели на экземпляры классов джобов;
- дополнительные опции (`TOperationOptions`).

Обязательно добавьте пути ([TRichYPath](#trichypath)) ко входным и выходным таблицам. Поскольку в общем случае (в частности, в protobuf бэкенде) типы записей в таблицах могут быть разными, функции `AddInput()` и `AddOutput()` у спецификации шаблонные:

```c++
TMapOperationSpec spec;
spec.AddInput<TYaMRRow>("//tmp/table")
    .AddInput<TYaMRRow>("//tmp/other_table")
    .AddOutput<TNode>("//tmp/output_table");
```

Расхождение типов спецификации с типами, указанными в IMapper/IReducer, будет по возможности отловлено в рантайме.

Непосредственный вызов без дополнительных параметров выглядит так:

```c++
client->Map(spec, new TExtractKeyMapper);
```

Это безопасно, поскольку внутри на объект джоба создаётся `TIntrusivePtr`.

### Передача пользовательских файлов

Для передачи в sandbox джоба пользовательских файлов используется структура `TUserJobSpec`, входящая в состав основной спецификации. Наборы файлов могут быть разными для разных типов джобов в одной операции:

```c++
TMapReduceOperationSpec spec;
spec.MapperSpec(TUserJobSpec().AddLocalFile("./file_for_mapper"))
    .ReducerSpec(TUserJobSpec().AddLocalFile("./file_for_reducer"));
```

Для использования файлов, уже имеющихся в системе, нужно вызвать метод `TUserJobSpec::AddFile(fileName)`.

Важным дополнительным параметром является `TOperationOptions::Spec`. Это `TNode`, который может содержать любые параметры спецификации, объединяемые с основной спецификацией, формируемой клиентом. Многие из этих параметров описаны в разделе [Типы операций](../../../user-guide/data-processing/operations/overview.md).

### Несколько таблиц на входе и выходе

Если операция пишет в несколько выходных таблиц, используется метод `AddRow()` c двумя параметрами:

```c++
writer->AddRow(row, tableIndex);
```

Если операция читает из нескольких таблиц, индекс таблицы может быть получен из `TTableReader` с помощью метода `GetTableIndex()`.

### Особенности protobuf бэкенда

При использовании protobuf есть возможность задавать разные типы записей для входных и выходных таблиц. В этом случае в объявлении базового класса джоба используется `TTableReader` или `TTableWriter` не от конкретного пользовательского protobuf класса, а от базового `Message`:

```c++
class TProtoReducer
    : public IReducer<TTableReader<Message>, TTableWriter<Message>>
{ ... }
```

При этом в спецификации операции не следует писать `AddInput/AddOutput()`: необходимо указывать конкретный тип.

В этом случае методы `GetRow()` и `AddRow()` становятся шаблонными и должны использовать конкретные пользовательские типы. При чтении выбор вызываемой функции можно делать, применяя `GetTableIndex()` текущей записи перед вызовом `GetRow()`. При записи вызывается `AddRow(row, tableIndex)`. Есть контроль соответствия типов индексам таблиц. 

### Инициализация и сериализация джобов

Методы `Start()` и `Finish()` похожи на аналоги из общего интерфейса, принимают указатель на соответствующий `TTableWriter` в качестве параметра.

```c++
virtual void Save(IOutputStream& stream) const;
virtual void Load(IInputStream& stream);
```

Сериализация отвязана от каких-либо фреймворков. Содержимое сериализованного джоба сохраняется в sandbox в файле `jobstate`. Если при `Save()` не было записано ни одного байта, файл будет отсутствовать. `Load()` в джобе, тем не менее, будет вызван от пустого потока.

Есть хелпер `Y_SAVELOAD_JOB`, который оборачивает сериализацию из `util/ysaveload.h`.

Порядок вызова методов в джобе: `default ctor; Load(); Start(); Do(); Finish()`.

### Подготовка операции из класса джоба { #prepare_operation }

В классе `IJob` (базовом классе для всех джобов) добавлен виртуальный метод `void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const`, который можно перегружать в своих джобах:

- `context` позволяет получать информацию о входных и выходных таблицах, в частности, их схемах.
- `preparer` нужен для собственно контроля над параметрами спецификации.

Можно задавать:
  - Схему выходных таблиц: `preparer.OutputSchema(tableIndex, schema)`.
  - Типы сообщений для входного и выходного формата protobuf: `preparer.InputDescription<TYourMessage>(tableIndex)` и `preparer.OutputDescription<TYourMessage>(tableIndex)`.
  - Фильтрацию колонок входных таблиц: `preparer.InputColumnFilter(tableIndex, {"foo", "bar"})`.
  - Возможность переименования колонок входных таблиц: `preparer.InputColumnRenaming(tableIndex, {{"foo", "not_foo"}})`. Переименование применяется *после* фильтрации.

Все методы возвращают `*this`, что позволяет использовать chaining.

Обычно при использовании protobuf достаточно указывать только `.OutputDescription`, схема выходной таблицы выведется автоматически.
Если это поведение не устраивает, можно указать вторым параметром (`inferSchema`) `false`, например, `preparer.OutputDescription<TRow>(tableIndex, false)`.
Для джобов, размечающих `OutputDescription`, можно использовать короткие варианты запуска операций вроде `client->Map(new TMapper, input, output)`.

Если есть несколько похожих таблиц, их описание можно объединять в группы, используя методы `.(Begin|End)(Input|Output)Group`.
`.Begin(Input|Output)Group` принимает описание индексов таблиц, которые объединяет данная группа.
Это может быть либо пара `[begin, end)`, либо контейнер с `int`, например:

```c++
preparer
    .BeginInputGroup({0,1,2,3})
        .Description<TInputRow>()
        .ColumnFilter({"foo", "bar"})
        .ColumnRenaming({{"foo", "not_foo"}})
    .EndInputGroup()
    .BeginOutputGroup(0, 7)
        .Description<TOutputRow>()
    .EndOutputGroup();
```


### Замечания

- Функция `IMapper::Do()` принимает на вход все записи, попадающие в данный джоб. Это отличается от логики общего интерфейса.
- Функция `IReducer::Do()` принимает диапазон записей по одному ключу, как и раньше. В случае операции Reduce это определяется параметром спецификации `ReduceBy`, в случае JoinReduce — `JoinBy`.
- Если класс джоба шаблонный от пользовательских параметров, необходимо вызвать `REGISTER_*` на каждый инстанс, который необходимо запустить на кластере.

## Получение информации об операциях и джобах

### GetOperation

Получить [информацию об операции](../../../api/commands.md#get_operation).

```c++
TOperationAttributes GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
```

| **Параметр**                          | **Тип**                                 | **Значение по умолчанию** | **Описание**               |
| --------------------------------- | ----------------------------------- | ------- | ---------------------- |
| `operationId`                     |                                     |         | Id операции.            |
| `options` — необязательные опции: |                                     |         |                        |
| `AttributeFilter`                 | `TMaybe<TOperationAttributeFilter>` | none    | Какие атрибуты вернуть. |

### ListOperations

Получить список операций, [удовлетворяющих фильтрам](../../../api/commands.md#list_operation).

```c++
TListOperationsResult ListOperations(
    const TListOperationsOptions& options)
```

| **Параметр**                          | **Тип**                        | **Значение по умолчанию** | **Описание**                                                     |
| --------------------------------- | -------------------------- | ------- | ------------------------------------------------------------ |
| `options` — необязательные опции: |                            |         |                                                              |
| `FromTime`                        | `TMaybe<TInstant>`         | none    | Начало временн*о*го интервала поиска.                         |
| `ToTime`                          | `TMaybe<TInstant>`         | none    | Конец временн*о*го интервала поиска.                          |
| `CursorTime`                      | `TMaybe<TInstant>`         | none    | Положение курсора (для пагинации).                            |
| `CursorDirection`                 | `TMaybe<ECursorDirection>` | none    | С какого конца временн*о*го интервала начинать перечислять операции. |
| `Filter`                          | `TMaybe<TString>`          | none    | "Текстовые факторы" (в частности, title и пути до входных/выходных таблиц) операции включают `Filter` как подстроку. |
| `Pool`                            | `TMaybe<TString>`          | none    | Операция запущена в пуле `Pool`.                              |
| `User`                            | `TMaybe<TString>`          | none    | Операция запущена пользователем `User`.                       |
| `State`                           | `TMaybe<TString>`          | none    | Операция находится в состоянии `State`.                       |
| `Type`                            | `TMaybe<EOperationType>`   | none    | Операция имеет тип `Type`.                                    |
| `WithFailedJobs`                  | `TMaybe<bool>`             | none    | У операции есть (`WithFailedJobs == true`) или нет (`WithFailedJobs == false`) failed джобов |
| `IncludeArchive`                  | `TMaybe<bool>`             | none    | Искать ли операции в архиве операций.                            |
| `IncludeCounters`                 | `TMaybe<bool>`             | none    | Включать ли в ответ статистику по общему количеству (не только по заданному интервалу) операций. |
| `Limit`                           | `TMaybe<i64>`              | none    | Возвращать не более `Limit` операций (текущее максимальное значение этого параметра `100`, оно же значение по умолчанию). |

### UpdateOperationParameters

Обновить runtime parameters [запущенной операции](../../../api/commands.md#update_operation_parameters).

```c++
void UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
```

| **Параметр**                          | **Тип**                                     |  Значение по умолчанию | **Описание**                                                     |
| --------------------------------- | --------------------------------------- | ------- | ------------------------------------------------------------ |
| `operationId`                     | Id операции                             |         |                                                              |
| `options` — необязательные опции: |                                         |         |                                                              |
| `Owner`                           | `TVector<TString>`                      | none    | Новые владельцы операции.                                     |
| `Pool`                            | `TMaybe<TString>`                       | none    | Перенести операцию в пул `Pool` (для всех pool trees, для которых она запущена). |
| `Weight`                          | `TMaybe<double>`                        | none    | Новый вес операции (для всех pool trees, для которых она запущена). |
| `SchedulingOptionsPerPoolTree`    | `TMaybe<TSchedulingOptionsPerPoolTree>` | none    | Опции планировщика для каждого pool tree. |

### GetJob

Получить [информацию о джобе](../../../api/commands.md#get_job).

```c++
TJobAttributes GetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
```

| **Параметр**                           | **Описание**    |
| ---------------------------------- | ----------- |
| `operationId`                      | Id операции |
| `jobId`                            | Id джоба    |
| `options` —  необязательные опции: |             |

### ListJobs

Получить джобы, [удовлетворяющие указанным фильтрам](../../../api/commands.md#list_jobs).

```c++
TListJobsResult ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
```

| **Параметр**                          | **Тип**                           | **Значение по умолчанию** | **Описание**                                                     |
| --------------------------------- | ----------------------------- | ------- | ------------------------------------------------------------ |
| `operationId`                     |                               |         | Id операции.                                                  |
| `options` — необязательные опции: |                               |         |                                                              |
| `Type`                            | `TMaybe<EJobType>`            | none    | Джоб имеет тип `Type`.                                        |
| `State`                           | `TMaybe<EJobState>`           | none    | Джоб находится в состоянии `State`.                           |
| `Address`                         | `TMaybe<TString>`             | none    | Джоб запущен (или был запущен) на узле `Address`.                     |
| `WithStderr`                      | `TMaybe<bool>`                | none    | Записал ли джоб что-нибудь в stderr.                          |
| `WithSpec`                        | `TMaybe<bool>`                | none    | Сохранена ли спецификация джоба.                                     |
| `WithFailContext`                 | `TMaybe<bool>`                | none    | Сохранён ли fail context джоба.                               |
| `WithMonitoringDescriptor`        | `TMaybe<bool>`                | none    | Выдался ли monitoring descriptor джобу.                               |
| `SortField`                       | `TMaybe<EJobSortField>`       | none    | По какому полю сортировать джобы в ответе. |
| `SortOrder`                       | `TMaybe<ESortOrder>`          | none    | Сортировать джобы по возрастанию или убыванию.                      |
| `DataSource`                      | `TMaybe<EListJobsDataSource>` | none    | Где искать джобы: в контроллер-агенте и Кипарисе (`Runtime`), в архиве джобов (`Archive`), автоматически в зависимости от наличия операции в Кипарисе (`Auto`) или как-то иначе (`Manual`). |
| `IncludeCypress`                  | `TMaybe<bool>`                | none    | Искать ли джобы в Кипарисе (опция учитывается для `DataSource == Manual`). |
| `IncludeControllerAgent`          | `TMaybe<bool>`                | none    | Искать джобы в контроллер-агенте (опция учитывается для `DataSource == Manual`). |
| `IncludeArchive`                  | `TMaybe<bool>`                | none    | Искать ли джобы в архиве джобов (опция учитывается для `DataSource == Manual`). |
| `Limit`                           | `TMaybe<i64>`                 | none    | Вернуть не более `Limit` джобов.                              |
| `Offset`                          | `TMaybe<i64>`                 | none    | Пропустить первые `Offset` джобов (в порядке сортировки).     |

### GetJobInput

Получить вход [запущенного или упавшего джоба](../../../api/commands.md#get_job_input).

```c++
IFileReaderPtr GetJobInput(
    const TJobId& jobId,
    const TGetJobInputOptions& options)
```

| **Параметр**                                    | **Описание** |
| ------------------------------------------- | -------- |
| `jobId`                                     | Id джоба. |
| `options` — необязательные опции (пока нет) |          |

### GetJobFailContext

Получить fail context [упавшего джоба](../../../api/commands.md#get_job_fail_context).

```c++
IFileReaderPtr GetJobFailContext(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options)
```

| **Параметр**                                 | **Описание**    |
| -------------------------------------------- | ----------- |
| `operationId`                                | Id операции. |
| `jobId`                                      | Id джоба.    |
| `options` —  необязательные опции (пока нет) |             |

### GetJobStderr

Получить stderr [запущенного или упавшего джоба](../../../api/commands.md#get_job_stderr).

```c++
IFileReaderPtr GetJobStderr(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options)
```

| **Параметр**                                    | **Описание**    |
| ------------------------------------------- | ----------- |
| `operationId`                               | Id операции. |
| `jobId`                                     | Id джоба.    |
| `options` — необязательные опции (пока нет) |             |

## Работа с файловым кэшом

На кластерах есть общий файловый кеш, хранящий файлы по ключу — их MD5-хэшу. Сейчас
он находится по пути `//tmp/yt_wrapper/file_storage/new_cache`.
Для работы с ним можно использовать методы `PutFileToCache` и `GetFileFromCache`.
Эти же методы можно использовать и для работы со своим кешем, живущим по другому пути (однако очищать его придётся также самостоятельно).

{% note warning %}

Все команды для работы с файловым кешем нетранзакционны, поэтому являются методами `IClient`, но не `IClientBase` и не `ITransaction`.

{% endnote %}


### PutFileToCache

[Скопировать файл в кеш](../../../api/commands.md#put_file_to_cache). Возвращает путь к файлу в кеше.

```c++
TYPath PutFileToCache(
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options)
```

| **Параметр**                                    | **Описание**                                                     |
| ------------------------------------------- | ------------------------------------------------------------ |
| `filePath`                                  | Путь к файлу в Кипарисе (файл должен иметь атрибут `md5`, для этого он должен быть записан с опцией `TFileWriterOptions::ComputeMD5`). |
| `md5Signature`                              | MD5-хэш файла.                                                |
| `cachePath`                                 | Путь в Кипарисе к корню файлового кеша.                       |
| `options` — необязательные опции (пока нет) |                                                              |

### GetFileFromCache

[Получить путь к файлу в кеше](../../../api/commands.md#get_file_from_cache). Возвращает путь или `Nothing`, если файла с таким MD5 нет.

```c++
TMaybe<TYPath> GetFileFromCache(
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options);
```

| **Параметр**                                    | **Описание**                               |
| ------------------------------------------- | -------------------------------------- |
| `md5Signature`                              | MD5-хэш файла.                          |
| `cachePath`                                 | Путь в Кипарисе к корню файлового кеша. |
| `options` — необязательные опции (пока нет) |                                        |

## Переменные окружения

Сейчас эти настройки доступны только из переменных окружения и одни и те же для всех экземпляров `IClient`. однако мы собираемся переработать эту функциональность, чтобы можно было почти всё настраивать непосредственно из кода и менять на уровне отдельных `IClient`.

| *Переменная*                          | *Тип*   | *Значение*                                                   |
| ------------------------------------- | ------- | ------------------------------------------------------------ |
| YT_TOKEN                              | string  | Значение пользовательского токена.                            |
| YT_TOKEN_PATH                         | string  | Путь к файлу с пользовательским токеном.                      |
| YT_PREFIX                             | string  | Путь, дописываемый префиксом ко всем путям в Кипарисе.        |
| YT_TRANSACTION                        | guid    | Id внешней транзакции. При наличии этого параметра `CreateClient()`будет создавать экземпляр `IClient`, работающий в контексте данной транзакции. |
| YT_POOL                               | string  | Пул для запуска пользовательских операций.                    |
| YT_FORCE_IPV4                         | bool    | Использовать только IPv4.                                     |
| YT_FORCE_IPV6                         | bool    | Использовать только IPv6.                                     |
| YT_CONTENT_ENCODING                   | string  | HTTP-заголовок Content-Encoding: поддерживается `identity`, `gzip`, `y-lzo`, `y-lzf`. |
| YT_ACCEPT_ENCODING                    | string  | HTTP-заголовок Accept-Encoding.                               |
| YT_USE_HOSTS                          | bool    | Использовать ли тяжёлые прокси.                              |
| YT_HOSTS                              | string  | Путь в запросе списка тяжёлых прокси.                        |
| YT_RETRY_COUNT                        | int     | Количество попыток на HTTP-запрос.                            |
| YT_START_OPERATION_RETRY_COUNT        | int     | Количество попыток старта операции при превышении лимита одновременно идущих операций. |
| YT_VERSION                            | string  | Версия HTTP API.                                              |
| YT_SPEC                               | json    | Спецификация, присоединяемая к каждой спецификации операции. Порядок: основная спецификация, `YT_SPEC`, `TOperationOptions::Spec`. |
| YT_CONNECT_TIMEOUT                    | seconds | Таймаут на подключение к сокету.                              |
| YT_SOCKET_TIMEOUT                     | seconds | Таймаут на работу с сокетом.                                  |
| YT_TX_TIMEOUT                         | seconds | Таймаут транзакции.                                           |
| YT_PING_INTERVAL                      | seconds | Интервал между последовательными пингами транзакции.          |
| YT_RETRY_INTERVAL                     | seconds | Интервал между попытками HTTP-запроса.                        |
| YT_RATE_LIMIT_EXCEEDED_RETRY_INTERVAL | seconds | Интервал между попытками в случае превышения лимита на request rate. |
| YT_START_OPERATION_RETRY_INTERVAL     | seconds | Интервал между попытками старта операции.                     |

## Потокобезопасность

`IClient` и `ITransaction` не имеют изменяемого состояния. Потокобезопасно.

{% if audience == "internal" %}

## Юнит-тесты

Юнит-тесты для {{product-name}}-операций оформляются с помощью целей UNITTEST или GTEST с добавлением в PEERDIR хука `yt/cpp/mapreduce/tests/yt_initialize_hook`. Потребуется также подключение библиотеки `yt/cpp/mapreduce/tests/yt_unittest_lib`, из которой надо использовать в тестах метод `CreateTestClient()` вместо обычного `CreateClient()`, а также рецепта `mapreduce/yt/python/recipe/recipe.inc`, который создает локальную песочницу {{product-name}}, в которую тестовый клиент и ходит. Если в тестах нужен архив операций, то вместо указанного рецепта нужно использовать `mapreduce/yt/python/init_operations_archive/recipe/recipe194.inc`.

В остальном это обычные юнит-тесты Arcadia.

К сожалению, локальная песочница на данный момент запускается весьма долго, поэтому обычно юнит-тесты нескольких библиотек объединяют в один большой тест и ставят ему размер `MEDIUM`.

Каркас файла ya.make для цели UNITTEST:

```
UNITTEST()

PEERDIR(
    yt/cpp/mapreduce/tests/yt_initialize_hook
    yt/cpp/mapreduce/tests/yt_unittest_lib
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
```

Аналогично для цели GTEST:

```
GTEST()

PEERDIR(
    yt/cpp/mapreduce/tests/yt_initialize_hook
    yt/cpp/mapreduce/tests/yt_unittest_lib
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
```

## Использование старого и нового C++ API одновременно

Использование старого YaMR API и нового C++ API в одной программе в принципе возможно, хотя не поощряется. Возможный случай для такого смешивания: плавный перевод программы с одного API на другой.

В начале программы должна быть произведена инициализация обоих API, например так:

```c++
int main(int argc, const char** argv) {
    NMR::Initialize(argc, argv);
    NYT::Initialize();
    ...
}
```

{% else %}{% endif %}
