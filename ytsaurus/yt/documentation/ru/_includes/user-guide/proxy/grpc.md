# gRPC-прокси

Существует возможность общаться с {{product-name}} кластером по gRPC протоколу.

Плюсы данной возможности:

1. Полноценная поддержка динамических таблиц, есть возможность работать с транзакциями.
2. Поддержка всех основных команд для работы с Кипарисом.
3. gRPC — промышленный стандарт для общения с сервисом, легко устанавливать через pip или deb-пакеты. Также имеется документация в интернете.
   Минусы:
4. Протокол более низкоуровневый. Более сложная работа с табличными данными, требуется сериализовывать/десериализовывать строки вручную.
5. В данный момент отсутствует интеграции с готовыми HTTP клиентами {{product-name}} (C++ или Python).
6. Написание полноценной удобной клиентской библиотеки поверх API — это сложная и объемная задача. Для части языков (C++, Python, Java) она отчасти решена нами. 

gRPC запросы обслуживают [RPC-прокси](../../../user-guide/proxy/rpc.md) {{product-name}}. Как следует из названия прокси (RPC) — от клиента сервер ожидает [protobuf](https://developers.google.com/protocol-buffers/) сообщение. Ответом сервера также будет сообщение. Каждая команда {{product-name}}, такая как «создать узел в Кипарисе», «записать строки» представлена в виде пары сообщений (с префиксами TReq и TRsp для запроса и ответа соответственно). Пример: `TReqCreateNode, TRspCreateNode` —  создание узла в Кипарисе. Сообщения и их описания можно посмотреть в proto файле.

## Установка и компиляция proto сообщений

Скомпилированные сообщения можно установить через pip: `pip install ytsaurus-proto`. Также необходимо установить gRPC и protobuf следующих версий (более свежие версии тоже можно, но на более свежих пакетах функциональность не тестировалась): 

   ```
   protobuf>=3.2.1
   grpcio==1.2.0rc1
   ```

## Примеры

Короткий пример get запроса к Кипарису через gRPC:

```python
import yt_proto.yt.client.api.rpc_proxy.proto.api_service_pb2 as api_service_pb2

import yt.yson as yson

import yt.wrapper as yt

import grpc
import random

if __name__ == "__main__":
    # Будем делать запрос к кластеру 
    yt.config["proxy"]["url"] = "cluster-name"

    # Получаем список gRPC прокси, команда "discover_proxies" доступна с четвертой версии API.
    yt.config["api_version"] = "v4"
    proxies = yt.driver.make_formatted_request("discover_proxies", {"type": "grpc"}, format=None)["proxies"]

    # Создаем gRPC соединение к случайной прокси из списка.
    channel = grpc.insecure_channel(str(random.choice(proxies)))

    # Заполняем proto сообщение, делаем get на путь //home/username.
    get_req = api_service_pb2.TReqGetNode(path="//home/username")

    # Передаем минимальную версию протокола (подробнее об этом ниже) и токен доступа.
    metadata = [
        ("yt-protocol-version", "1.0"),
        ("yt-auth-token", yt._get_token())
    ]

    # Инициируем unary-unary запрос (то есть одно сообщение от клиента - запрос и одно сообщение от сервера - ответ).
    unary = channel.unary_unary(
        "/ApiService/GetNode",
        request_serializer=api_service_pb2.TReqGetNode.SerializeToString,
        response_deserializer=api_service_pb2.TRspGetNode.FromString)

    # Делаем запрос.
    _, call = unary.with_call(get_req, metadata=metadata)

    # Печатаем ответ.
    print yson.loads(call.result().value)
```

## Версионирование сообщений и версия протокола

Клиент должен передавать `yt-protocol-version` в заголовках каждого запроса. Это поле представляет собой строку вида "Major.Minor". При получении запроса gRPC сервер проверяет следующее:

1. Major версия клиента и сервера должны в точности совпадать.
2. Minor версия сервера должна быть больше или равна Minor версии клиента.
   Если условия не выполнены, то будет ошибка.

Major версия может меняться при серьезных изменениях протокола и такие изменения будут редки. Внутри одной Major версии поддерживается обратная совместимость: старые клиенты будут работать с новыми версиями сервера.

Minor версия может меняться при пересборке пакета со скомпилированными proto файлами (например, при добавлении опциональных полей в proto сообщения). На плечах клиента лежит ответственность за указание нужной версии протокола. Например, допустим, в сообщение есть optional параметр:

```
// Since 1.42
optional int64 some_important_value = 42;
```

Этот параметр, как можно видеть из комментария, появился в версии сервера "1.42". Если вы, как клиент, этот параметр используете, то необходимо указать `yt-protocol-version = "1.42"`, иначе можно получить спецэффект: старый сервер просто проигнорирует этот параметр.

## Протокол gRPC в {{product-name}}

{{product-name}} использует протокол gRPC следующим образом:

1. В {{product-name}} не используются gRPC описания сервисов в proto файлах.
2. В {{product-name}} к proto сообщению может добавляться произвольный блоб бинарных данных (так называемый attachment, про это ниже).

Эти отличия не требуют модификации кода gRPC, можно использовать обычный стоковый клиент.

### Attachment в gRPC

Для передачи бинарных данных (таких как строки для вставки в динамическую таблицу) {{product-name}} добавляет поддержку так называемых attachments.

Attachment - блок бинарных данных, привязанный к одному сообщению. Attachment может быть как в запросе (например, строки для вставки), так и в ответе. В ответе и запросе может быть несколько attachments.

Для формального описания обратимся к [описанию](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md) протокола gRPC поверх HTTP/2 и рассмотрим пример запроса и ответа.

**Запрос**

```
HEADERS (flags = END_HEADERS)
:method = POST
:scheme = http
:path = /google.pubsub.v2.PublisherService/CreateTopic
:authority = pubsub.googleapis.com
grpc-timeout = 1S
content-type = application/grpc+proto
grpc-encoding = gzip
authorization = Bearer y235.wef315yfh138vh31hv93hv8h3v

DATA (flags = END_STREAM)
<Length-Prefixed Message>
```

**Ответ**

```
HEADERS (flags = END_HEADERS)
:status = 200
grpc-encoding = gzip
content-type = application/grpc+proto

DATA
<Length-Prefixed Message>

HEADERS (flags = END_STREAM, END_HEADERS)
grpc-status = 0 # OK
trace-proto-bin = jher831yy13JHy3hc
```

Как в запросе, так и в ответе есть различная метаинформация (`grpc-encoding`, `content-type`, `grpc-status` и тд). Секция данных, которая помечена как `DATA <Length-Prefixed Message>`, описывается в нотации [ABNF](https://en.wikipedia.org/wiki/Augmented_Backus–Naur_form).

```
Length-Prefixed-Message → Compressed-Flag Message-Length Message
Compressed-Flag → 0 / 1 # encoded as 1 byte unsigned integer
Message-Length → {length of Message} # encoded as 4 byte unsigned integer
Message → *{binary octet}
```

Это флаг сжатия (занимает один байт, в случае с RPC Proxy {{product-name}} он должен быть всегда равен 0), затем идет длина сообщения, кодированная четырьмя байтами, а затем следует сообщение в виде произвольной байтовой последовательности длины `Message-Length`.

{{product-name}} уточняет Message, теперь это не произвольная байтовая последовательность. Сообщение описывается следующим образом:

```
Message → SerializedProtoMessage *Length-Prefixed-Attachment
Length-Prefixed-Attachment → Attachment-Length [Attachment]
Attachment-Length → {length of Attachment} # encoded as 4 byte unsigned integer, can be 0xFFFFFFFF if Attachment is omitted 
Attachment → *{binary octet} 
```

Сначала идет часть `SerializedProtoMessage`, которая представляет собой сериализованное proto сообщение. Далее следует количество (возможно, нулевое) attachments. Каждый attachment - это длина (`Attachment-Length`, кодируется четырьмя байтами) + произвольная байтовая последовательность длины `Attachment-Length`.
Отметим, что если attachments нет, то протокол полностью идентичен стоковому gRPC протоколу.

Система не может отличить, где заканчивается `SerializedProtoMessage`. Для этого вводится специальный ключ в метаданных в заголовке: `yt-message-body-size`.

Таким образом, система вычитывает `yt-message-body-size` байт, превращает их в proto сообщение, а остальные байты до конца потока рассматриваются как attachments. Если заголовок `yt-message-body-size` не указан, то, в целях совместимости, весь поток считается сериализованным proto сообщением.

## Wire format

Для передачи строк {{product-name}} использует wire format, сериализованные строки передаются в attachments. Разбиение сериализованного потока строк на аттачменты произвольно, стоит рассматривать несколько последовательных attachments как один поток байт.

Каждый запрос, который может принимать или возвращать строки, имеет [rowset descriptor](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto). Дескриптор описывает, каким образом строки должны быть десериализованы или сериализованы. В данной секции описывается, как устроен unversioned rowset (`RK_UNVERSIONED`). Если вы хотите использовать `RK_VERSIONED` или `RK_SCHEMAFUL` тип, то напишите на рассылку {{%if lang == ru%}}[yt@](mailto:community_ru@ytsaurus.tech){{% else %}}[yt@](mailto:community@ytsaurus.tech){{% endif %}}.

Для описания воспользуемся уже знакомой ABNF нотацией (endianness - little):

```
UnversionedRowset → RowCount *UnversionedRow
RowCount → {row count} # encoded as 8 byte unsigned integer
UnversionedRow → ValueCount *UnversionedValue
ValueCount → {value count} # encoded as 8 byte unsigned integer
UnversionedValue → ValueIndex ValueType AggregateFlag Length Content
ValueIndex → {index of column to which value belong, should match `columns` field in TColumnDescriptor} # encoded as 2 byte unsigned integer
ValueType → 0x02 {Null} / 0x03 {Int64} / 0x04 {Uint64} / 0x05 {Double} / 0x06 {Boolean} / 0x10 {String} / 0x11 {Any} # encoded as unsigned byte
AggregateFlag → {is value aggregate?} # encoded as unsigned byte
Length → {length of Content} # encoded as 4 byte unsigned integer
Content → *{binary octet} # must be 8-byte aligned!
```

Таким образом, минимальная содержательная длина `Content` может быть 8 байт (для всех значений, кроме String и Any).

Для примера рассмотрим словарь `{"a": 1, "b": {"x": "y"}}`. Это одна строка (`UnversionedRow` в терминах выше), состоящая из двух `UnversionedValue`, одно (ключ `a`) типа `int64`, другое (ключ `b`) типа `any`.

Имена колонок не передаются в строках, они передаются в `TColumnDescriptor` в proto сообщении, а в строках есть только их индекс `ValueIndex`.

Пример реализации такого формата в Python можно найти [тут](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/yt/wire_format).
