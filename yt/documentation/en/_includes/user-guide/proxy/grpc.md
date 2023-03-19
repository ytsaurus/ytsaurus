# gRPC proxy

You can communicate with a {{product-name}} cluster via the gRPC protocol.

Advantages:

1. Full support for dynamic tables, you can work with transactions.
2. Support for all basic commands for working with Cypress.
3. gRPC is an industry standard for communicating with the service and it is easy to install it via pip or deb packages. Documentation is also available on the internet.
   Disadvantages:
4. The protocol is more low-level. More complicated work with tabular data, you need to serialize/deserialize rows manually.
5. There is currently no integration with off-the-shelf {{product-name}} HTTP clients (C++ or Python).
6. Creating a full-fledged user-friendly client library on top of the API is a complicated and time-consuming task. For some languages (C++, Python, and Java), we have partially solved it.

gRPC queries serve {{product-name}} [RPC proxies](../../../user-guide/proxy/rpc.md). As follows from the proxy name (RPC), the server expects a [protobuf](https://developers.google.com/protocol-buffers/) from the client. The server will also respond with a message. Each {{product-name}} command, such as "create a node in Cypress" or "write rows", is represented as a pair of messages (with TReq and TRsp prefixes for a query and a response respectively). Example: `TReqCreateNode, TRspCreateNode`: Create a node in Cypress. You can view the messages and their descriptions in the proto file.

## Installing and compiling proto messages

You can install compiled messages via pip: `pip install ytsaurus-proto`. You also need to install gRPC and protobuf of the following versions (newer versions are also allowed, but functionality has not been tested on newer packages):

```
protobuf>=3.2.1
grpcio==1.2.0rc1
```

## Examples

A short example of a get query to Cypress via gRPC:

```python
import yt_proto.yt.client.api.rpc_proxy.proto.api_service_pb2 as api_service_pb2

import yt.yson as yson

import yt.wrapper as yt

import grpc
import random

if __name__ == "__main__":
    # We will query the cluster
    yt.config["proxy"]["url"] = "cluster-name"

    # Getting a list of gRPC proxies, the "discover_proxies" command is available from the fourth version of the API.
    yt.config["api_version"] = "v4"
    proxies = yt.driver.make_formatted_request("discover_proxies", {"type": "grpc"}, format=None)["proxies"]

    # Creating a gRPC connection to an arbitrary proxy from the list.
    channel = grpc.insecure_channel(str(random.choice(proxies)))

    # Filling in the proto message, making a get on the path //home/username.
    get_req = api_service_pb2.TReqGetNode(path="//home/username")

    # Transmitting the minimum version of the protocol (learn more about this below) and the access token.
    metadata = [
        ("yt-protocol-version", "1.0"),
        ("yt-auth-token", yt._get_token())
    ]

    # Initiating a unary-unary query (that is, one message from the client (the query) and one message from the server (the response)).
    unary = channel.unary_unary(
        "/ApiService/GetNode",
        request_serializer=api_service_pb2.TReqGetNode.SerializeToString,
        response_deserializer=api_service_pb2.TRspGetNode.FromString)

    # Making a query.
    _, call = unary.with_call(get_req, metadata=metadata)

    # Typing a response.
    print yson.loads(call.result().value)
```

## Message versioning and protocol version

The client must pass `yt-protocol-version` in the headers of each query. This field is a row of the "Major.Minor" form. When receiving a gRPC query, the server checks the following:

1. The Major version of the client and the server must match exactly.
2. The Minor version of the server must be greater than or equal to the Minor version of the client.
   If the conditions are not met, an error will occur.

The Major version may change in case of major protocol changes and such changes will be rare. Backward compatibility is supported within a single Major version: old clients will work with new versions of the server.

The Minor version can change when a package with compiled proto files is rebuilt (for example, when optional fields are added to proto messages). The client is responsible for specifying the correct version of the protocol. Let's say there is an optional parameter in the message:

```
// Since 1.42
optional int64 some_important_value = 42;
```

As you can see from the comment, this parameter appeared in the server version "1.42". If you are using this parameter as a client, you need to specify `yt-protocol-version = "1.42"`, otherwise the old server may simply ignore this parameter.

## gRPC protocol in {{product-name}}

{{product-name}} uses the gRPC protocol as follows:

1. In {{product-name}}, gRPC descriptions of services in proto files are not used.
2. In {{product-name}}, an arbitrary blob of binary data (the so called attachment, learn more about this below) can be added to the proto message.

These differences do not require modification of the gRPC code, you can use an ordinary stock client.

### Attachment in gRPC

To transfer binary data (such as rows to be inserted into a dynamic table), {{product-name}} adds support for so called attachments.

Attachment is a block of binary data attached to a single message. It can be used both in the query (such as rows to be inserted) and in the response. There may be multiple attachments in the response and the query.

For a formal description, let's turn to the [description](https://github.com/grpc/grpc/blob/master/doc/PROTOCOL-HTTP2.md) of gRPC over HTTP/2 and consider an example of the query and response.

**Query**

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

**Response**

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

Both the query and the response contain various metainformation (`grpc-encoding`, `content-type`, `grpc-status`, and others). The data section marked as `DATA <Length-Prefixed Message>` is described in the [ABNF](https://en.wikipedia.org/wiki/Augmented_Backus–Naur_form) notation.

```
Length-Prefixed-Message → Compressed-Flag Message-Length Message
Compressed-Flag → 0 / 1 # encoded as 1 byte unsigned integer
Message-Length → {length of Message} # encoded as 4 byte unsigned integer
Message → *{binary octet}
```

This is the compression flag (it takes one byte, in case of the {{product-name}} RPC proxy, it must always be 0) followed by the message length encoded by four bytes and the message as an arbitrary byte sequence of `Message-Length`.

{{product-name}} specifies the Message, now it is not an arbitrary byte sequence. The message is described as follows:

```
Message → SerializedProtoMessage *Length-Prefixed-Attachment
Length-Prefixed-Attachment → Attachment-Length [Attachment]
Attachment-Length → {length of Attachment} # encoded as 4 byte unsigned integer, can be 0xFFFFFFFF if Attachment is omitted
Attachment → *{binary octet}
```

First comes the `SerializedProtoMessage` part that is a serialized proto message. This is followed by the number of (possibly zero) attachments. Each attachment is a length (`Attachment-Length` encoded by four bytes) + an arbitrary byte sequence of `Attachment-Length`.
Note that if there are no attachments, the protocol is completely identical to the stock gRPC protocol.

The system cannot distinguish where `SerializedProtoMessage` ends. To do this, enter a special key in the metadata in the header: `yt-message-body-size`.

Thus, the system reads `yt-message-body-size` bytes, turns them into a proto message, and considers the remaining bytes until the end of the stream as attachments. If the `yt-message-body-size` header is not specified, the entire stream is considered a serialized proto message for compatibility purposes.

## Wire format

{{product-name}} uses a wire format to transmit rows, serialized rows are transmitted to attachments. The partitioning of a serialized row stream into attachments is arbitrary, consider several consecutive attachments as a single byte stream.

Each query that can accept or return rows has a [rowset descriptor](https://github.com/ytsaurus/ytsaurus/blob/main/yt/yt_proto/yt/client/api/rpc_proxy/proto/api_service.proto). It describes how rows should be deserialized or serialized. This section describes how an unversioned rowset (`RK_UNVERSIONED`) is arranged. If you want to use the `RK_VERSIONED` or `RK_SCHEMAFUL` type, write to {{%if lang == ru%}}[yt@](mailto:community_ru@ytsaurus.tech){{% else %}}[yt@](mailto:community@ytsaurus.tech){{% endif %}}.

Use the familiar ABNF notation (endianness - little) for description:

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

Thus, the minimum `Content` length can be 8 bytes (for all values except String and Any).

Let's consider the `{"a": 1, "b": {"x": "y"}}` dict as an example. This is one row (`UnversionedRow` in the terms above) consisting of two `UnversionedValue` — one (key `a`) of the `int64` type and the other (key `b`) of the `any` type.

Column names are not passed in rows. They are passed in `TColumnDescriptor` in the proto message and the rows contain only their `ValueIndex`.

An example of implementing this format in Python is available [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/python/yt/wire_format).
