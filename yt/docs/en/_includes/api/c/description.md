# Description

To use the features of {{product-name}}, you can use the [C++ client](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce).

{% if audience == "internal" %}
See also [Use cases](../../../api/c/examples.md).{% else %}{% endif %}

{% if audience == "public" %}

{% note warning %}

Please note, the C++ client is provided as-is, so:
- the client interface can change without backward compatibility;
- only static linkage is currently supported;
- email info@ytsaurus.tech if you have questions about building the client.

{% endnote %}

{% endif %}

## General information

- The entire code is in the `NYT` namespace.
- All the interfaces that the user might need can be found [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/interface).
- The implementation of all the interfaces is available [here](https://github.com/ytsaurus/ytsaurus/tree/main/yt/cpp/mapreduce/client). Libraries need to be linked with `interface`, and the build (program) end goals must be linked with `client`.
- `ISomethingPtr` is usually defined as `TIntrusivePtr` for interfaces.
- All mandatory parameters are explicitly present in the signatures of the corresponding functions. All optional command parameters are presented as auxiliary structures within `TOptions`. These structures have a builder-like interface that allows for specifying parameters as `TWhateverOptions().Parameter1(value1).Parameter2(value2)` for single-line writing, if convenient.

## Logging { #logging }

Logging is toggled with the `YT_LOG_LEVEL` environment variable, which can take one of the following values: `ERROR`, `WARNING` (equivalent to `ERROR` for compatibility with the [Python API](../../../api/python/userdoc.md)), `INFO`, `DEBUG`.
In addition, you need to call the `NYT::Initialize()` method at the start of the program (see the next section).

If possible, saving logs at the `DEBUG` level is recommended, especially for production processes. Should any API issues occur, such logs will make fixing them much easier and faster. Attaching these logs to emails about API-related problems is recommended.

## Initializing { #init }

Similarly to the general interface, you need to call the `Initialize(argc, argv)` method before starting client actions.

This is where execution branches between the client code and the job code, and additional system initialization (logging, etc.) is performed.

{% note warning %}

Command line arguments with the `—yt*` prefix are reserved (for example, `--yt-reduce`).

{% endnote %}

If the program does not start operations, you can call `JoblessInitialize()`. This function will perform basic library initialization (logging, etc.), because it does not require passing `(argc, argv)`.

The client entry point is the `CreateClient(serverName)` function, which returns a pointer to the `IClient` interface.

Example of a minimum program (`main.cpp` file):

```c++
#include <mapreduce/yt/interface/client.h>
int main(int argc, const char* argv[])
{
    NYT::Initialize(argc, argv);
    auto client = NYT::CreateClient("cluster_name");
    client->Create("//tmp/table", NYT::NT_TABLE);
}
```

The corresponding `ya.make` file:

```
PROGRAM()

PEERDIR(mapreduce/yt/client)

SRCS(main.cpp)

END()
```

If you need to have clients working under different [authorization tokens](../../../user-guide/storage/auth.md) within the same program, you can use the following options:

```
auto client = CreateClient(serverName, TCreateClientOptions().Token(userToken));
```

## Transactions { #transactions }

There are two interfaces (`IClient` and `ITransaction`) that have almost the same set of methods inherited from `IClientBase`. In the first case, the corresponding commands are executed without any client transaction; in the second case, they are executed under a transaction of the specified class. To get an `ITransaction` instance, use the `IClientBase::StartTransaction()` method. When using the `YT_TRANSACTION` environment variable, even`IClient` starts working under the parent transaction.

Example of working with transactions:

```c++
#include <mapreduce/yt/interface/client.h>
int main(int argc, const char* argv[])
{
    NYT::Initialize(argc, argv);
    auto client = NYT::CreateClient("cluster_name");
    auto tx = client->StartTransaction();
    tx->Remove("//tmp/table");
    tx->Commit();
}
```

The transaction has explicit methods `Commit()` and `Abort()`. If the transaction is destroyed without one of these methods being explicitly called, an attempt will be made in the destructor to call `Abort()`, but without guarantees that the method will actually execute.

Transactions can be hierarchical:

```c++
auto tx = client->StartTransaction();
auto innerTx = tx->StartTransaction();
innerTx->Create("//tmp/double", NT_DOUBLE);
innerTx->Commit();
tx->Commit();
```

The `ITransaction::GetId()` method returns `TGUID` — the ID of this transaction.

The transaction created using `StartTransaction()` is automatically pinged by the C++ client and is fully controlled.

There is a way to work under an externally controlled transaction. To do this, call the `IClient::AttachTransaction(transactionId)` method. All commands called in such an object will be executed in the context of the transaction with this ID, but this transaction will not be pinged by the client.

For more information on transactions, see the [Transactions](../../../user-guide/storage/transactions.md) section.

## TNode { #tnode }

[TNode](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cpp/mapreduce/interface/node.h) is the main class that provides for a dynamic DOM representation of [YSON documents](../../../user-guide/storage/yson-docs.md). It is used for working with Cypress, transferring additional operation specifications, as one of the ways to encode table rows, and more.

Example of use:

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

- Values of primitive types are retrieved using the `TNode::AsType()` methods.
- Types can be checked using the `TNode::IsType()` methods.

By default, the constructor creates `TNode` with the `TNode::Undefined` type: such an object cannot be serialized in YSON without explicit initialization.

For shortening purposes, the methods for inserting in map and list immediately change `TNode` to the right type:

```c++
auto mapNode = TNode()("key1", "value1")("key2", "value2");
auto listNode = TNode().Add(100).Add(500);
```

## Working with Cypress { #cypress }

For more information about the commands, see [Working with the meta information tree](../../../user-guide/storage/cypress-example.md). Implemented as the `ICypressClient` interface, from which `IClientBase` is inherited.

Example of use:

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

Example of working with table attributes:

```c++
client->Set("//tmp/table/@user_attr", 25);

Cout << client->Get("//tmp/table/@row_count").AsInt64() << Endl;
Cout << client->Get("//tmp/table/@sorted").AsBool() << Endl;
```

### Create

[Create](../../../api/commands.md#create) a node. [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#create).

```c++
TNodeId Create(
    const TYPath& path,
    ENodeType type,
    const TCreateOptions& options);
```


| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Node path |
| `type` | `ENodeType` | - | [Type](../../../user-guide/storage/objects.md#object_types) of node created: <br/>  `NT_STRING` — string (`string_node`);<br/> `NT_INT64` — signed integer (`int64_node`); <br/>`NT_UINT64` — unsigned integer (`uint64_node`); <br/>`NT_DOUBLE` — double (decimal) (`double_node`); <br/>`NT_BOOLEAN` — Boolean value (`boolean_node`); <br/>`NT_MAP` — a Cypress dictionary (keys — strings, values — other nodes, `map_node`); <br/>`NT_LIST` — ordered list in (values — other nodes) (`list_node`);<br/> `NT_FILE` — [file](../../../user-guide/storage/objects.md#files) (`file`); <br/>`NT_TABLE` — [table](../../../user-guide/storage/objects.md#tables) (`table`);<br/> `NT_DOCUMENT` — [document](../../../user-guide/storage/objects.md#yson_doc) (`document`); <br/>`NT_REPLICATED_TABLE` — [replicated table](../../../user-guide/dynamic-tables/replicated-dynamic-tables.md) (`replicated_table`); <br/>`NT_TABLE_REPLICA` — (`table_replica`); |
| `options` &mdash; optional settings: | | |                                  |
| `Recursive` | `bool` | `false` | Whether to create intermediate directories. |
| `IgnoreExisting` | `bool` | `false` | If the node exists, do nothing and show no error. |
| `Force` | `bool` | `false` | If the node exists, re-create it instead of an error. |
| `Attributes` | `TNode` | none | [Attributes](../../../user-guide/storage/attributes.md) of the created node. |

### Remove

[Remove](../../../api/commands.md#remove) the node. [See equivalent in the {{product-name}}saurus](../../../user-guide/storage/cypress-example.md#create) utility.

```c++
void Remove(
    const TYPath& path,
    const TRemoveOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Path to the node. |
| `options` &mdash; optional settings: |  | |                                |
| `Recursive` | `bool` | `false` | Recursively remove the children of a composite node. |
| `Force` | `bool` | `false` | Do not stop if the specified node is no longer there. |

### Exists

[Checking the existence](../../../api/commands.md#exists) of the object at the specified path.

```c++
bool Exists(
    const TYPath& path)
```


| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Path to the node. |
| `options` &mdash; optional settings |  | |                                |

### Get

[Get the contents of the Cypress node](../../../api/commands.md#get) in `TNode` format. [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#get).

```c++
TNode Get(
    const TYPath& path,
    const TGetOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Path to the node or attribute. |
| `options` &mdash; optional settings: |  | |                                |
| `AttributeFilter` | `TMaybe<TAttributeFilter>` | none | List of attributes to be obtained. |
| `MaxSize` | `TMaybe<i64>` | none | Limit on the number of children (for composite nodes). |

### Set

[Write new Cypress node content.](../../../api/commands.md#set) [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#get).

```c++
void Set(
    const TYPath& path,
    const TNode& value,
    const TSetOptions& options);
```


| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Path to the node or attribute. |
| `value` | `TNode` | - | Written subtree (content). |
| `options` &mdash; optional settings: |  | |                                |
| `Recursive` | `bool` | `false` | Whether to create intermediate nodes. |

### List

[Get a list of the node's descendants](../../../api/commands.md#list). [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#list).

```c++
TNode::TListType List(
    const TYPath& path,
    const TListOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `path` | `TYPath` | - | Path to the node or attribute. |
| `options` &mdash; optional settings: |  | |                                |
| `AttributeFilter` | `TMaybe<TAttributeFilter>` | none | List of attributes to be received with each node. |
| `MaxSize` | `TMaybe<i64>` | none | Limit on the number of children (for composite nodes). |

### Copy

[Copy the Cypress node to a new address](../../../api/commands.md#copy). [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#copy_move).

{% note info "Note" %}

Copying is always recursive. The `Recursive` parameter lets you specify whether to create intermediate directories for `destinationPath` or not.

{% endnote %}


```c++
TNodeId Copy(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TCopyOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `sourcePath` | `TYPath` | - | Path to source node. |
| `destinationPath` | `TYPath` | - | Path for creating the copy (must not exist without the `Force` option). |
| `options` &mdash; optional settings: | | |                                  |
| `Recursive` | `bool` | `false` | Whether to create intermediate directories for `destinationPath`. |
| `Force` | `bool` | `false` | If `destinationPath` exists, replace with new content. |
| `PreserveAccount` | `bool` | `false` | Whether to save the accounts of the source nodes. |
| `PreserveExpirationTime` | `bool` | `false` | Whether to copy the `expiration_time` attribute. |
| `PreserveExpirationTimeout` | `bool` | `false` | Whether to copy the `expiration_timeout` attribute. |

### Move

[Move the node to a new path](../../../api/commands.md#move). [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#copy_move).

```c++
TNodeId Move(
    const TYPath& sourcePath,
    const TYPath& destinationPath,
    const TMoveOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `sourcePath` | `TYPath` | - | Path to source node. |
| `destinationPath` | `TYPath` | - | Path for creating the copy (must not exist without the `Force` option). |
| `options` &mdash; optional settings: | | |                                  |
| `Recursive` | `bool` | `false` | Whether to create intermediate directories for `destinationPath`. |
| `Force` | `bool` | `false` | If `destinationPath` exists, replace with new content. |
| `PreserveAccount` | `bool` | `false` | Whether to save the accounts of the source nodes. |
| `PreserveExpirationTime` | `bool` | `false` | Whether to copy the `expiration_time` attribute. |
| `PreserveExpirationTimeout` | `bool` | `false` | Whether to copy the `expiration_timeout` attribute. |

### Link

[Create](../../../api/commands.md#move) a [symbolic link](../../../user-guide/storage/links.md) to the object at the new address. [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#link).

```c++
TNodeId Link(
    const TYPath& targetPath,
    const TYPath& linkPath,
    const TLinkOptions& options)
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------|--------------|------------|---------------------------------- |
| `targetPath` | `TYPath` | - | Path to the source node where the link will direct. |
| `linkPath` | `TYPath` | - | Path where the link will be created (must not exist without the `Force` option). |
| `options` &mdash; optional settings | | |                                  |
| `Recursive` | `bool` | `false` | Whether to create intermediate directories. |
| `IgnoreExisting` | `bool` | `false` | If `linkPath` exists and *constitutes a link*, do nothing and show no error |
| `Force` | `bool` | `false` | If the `linkPath` node exists, create a link again in its place. |
| `Attributes` | `TNode` | none | [Attributes](../../../user-guide/storage/attributes.md) for `linkPath` in the form of `TNode` are written if created. |

### Concatenate

[Merge](../../../api/commands.md#concatenate) the set of files or tables (in the order in which their paths are listed). [See equivalent in the utility {{product-name}}](../../../user-guide/storage/cypress-example.md#concatenate).

The data merge takes place at the metadata level and exclusively on the [Cypress](../../../user-guide/storage/cypress.md) master server.

```c++
void Concatenate(
    const TVector<TYPath>& sourcePaths,
    const TYPath& destinationPath,
    const TConcatenateOptions& options);
```

| **Parameter** | **Type** | **Default value** | **Description** |
| ---------------------------|--------------|------------|---------------------------------- |
| `sourcePaths` | `TVector<TYPath>` | - | Paths to input files or tables. |
| `destinationPath` | `TYPath` | - | Path to the combined file or table, must exist. |
| `options` &mdash; optional settings: |  |            |                                   |
| `Append` | `bool` | `false` | Whether to keep the contents of `destinationPath`, add new data at the end. Without this flag, the old `destinationPath` content will disappear. |

## File reading and writing

To write a file to YT, you need to call the client/transaction method `CreateFileWriter(filePath)`.
The returned `IFileWriter` interface is inherited from `IOutputStream`. If the file does not already exist, it will be created.

```c++
auto writer = client->CreateFileWriter("//tmp/file");
*writer << "something";
writer->Finish();
```

Reading utilizes the `IFileReader` interface, which provides the `IInputStream` methods.

```c++
auto reader = client->CreateFileReader("//tmp/file");
TString anything;
*reader >> anything;
```

Additional options are passed through the `TFileWriterOptions` and `TFileReaderOptions` structures.

## Structures for representing table rows

There are several ways to represent individual table records in operations and during I/O.

### TNode

To represent a record, TNode must have the [map](../../../user-guide/storage/objects.md#primitive_types) type and constitute the display of column names as YSON values.

{% if audience == "internal" %}

### TYaMRRow

To work in a YaMR-compatible format:

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

For more information about Protobuf, see [Protobuf representation of tables](../../../api/c/protobuf.md).

Here, it is possible to define a custom protobuf type. To display protobuf fields in column names, use the following extensions:

```c++
import "mapreduce/yt/interface/protos/extension.proto";
message TSampleProto {
    optional int64  a = 1 [(NYT.column_name) = "column_a"];
    optional double b = 2 [(NYT.column_name) = "column_b"];
    optional string c = 3 [(NYT.column_name) = "column_c"];
}
```

There is support for nested structures, in which case binary protobuf serialization is used: a row with binary protobuf serialization of the [nested message](../../../api/c/protobuf.md#embedded) is stored in a table column. Nested structures can have `repeated` fields, which undergo binary serialization along with the rest of the nested message.

{% note warning %}

You should not use the `proto3` version. The [proto3 implementation](https://developers.google.com/protocol-buffers/docs/proto3#default) makes it so default field values are indistinguishable from missing fields. This results in value `0` of the `int` type, for example, not being written to the table.

{% endnote %}


## Table reading and writing

See [mapreduce/yt/interface/io.h](https://github.com/ytsaurus/YTsaurus//blob/main/mapreduce/yt/interface/io.h).


```c++
auto writer = client->CreateTableWriter<TNode>("//tmp/table");
writer->AddRow(TNode()("x", 1.)("y", 0.));
writer->AddRow(TNode()("x", 0.)("y", 1.));
writer->Finish();
```

Block writing. When `AddRow()` is called, the data is stored in the internal buffer, and if this buffer is larger than 64 MB, a separate thread is triggered that transmits the accumulated data.

The `Finish()` method is guaranteed to reset all the accumulated records or show an error. When destroying a writer without calling `Finish()`, a reset attempt will be made in the destructor without a guaranteed result. If the table did not exist before the creation of the writer, it will be created in the context of the same client or transaction with all the default attributes. If you need a different replication factor, you should create a table and give it the required attributes beforehand.

For reading, the main methods are `GetRow()`, `IsValid()`, `Next()`:

```c++
auto reader = client->CreateTableReader<TYaMRRow>("//tmp/ksv_table");
for (; reader->IsValid(); reader->Next()) {
    auto& row = reader->GetRow();
    Cout << row.Key << "; " << row.SubKey << "; " << row.Value << Endl;
}
```

The object returned from `GetRow()` can only be used before calling `Next()`.

The `GetRowIndex()` method allows you to get the absolute index of the current row.

Example with protobuf:

```c++
auto writer = client->CreateTableWriter<TSampleProto>("//tmp/table");
TSampleProto row;
row.set_a(42);
writer->AddRow(row);
```

### TRichYPath { #trichypath }

The `TRichYPath` structure allows you to pass various flags along with the path to the table. This applies to read and write functions as well as to input and output [operation](#operations) tables.

If you need to write in append mode:

```c++
client->CreateTableWriter(TRichYPath("//tmp/table").Append(true));
```

If you want the write result to be sorted, use the `SortedBy` attribute:

```c++
auto path = TRichYPath("//tmp/table").SortedBy({"a", "b"});
```

Based on the protobuf message, you can create a table schema:

```c++
auto path = WithSchema("//tmp/table");
```

If you have a schema, you can no longer specify sorting with `SortedBy`. You can specify sorting when creating a schema:

```c++
auto path = WithSchema("//tmp/table", {"a", "b"});
```

Or sort the schema later: `path.Schema_.SortBy({"a", "b"});`

Additionally, during reading `TRichYPath` allows for setting horizontal and vertical selections:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .LowerLimit(TReadLimit().RowIndex(10))
    .UpperLimit(TReadLimit().RowIndex(20)));
```

Synonymously, you can write:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange::FromRowIndices(10, 20));
```

If the table is sorted, you can set the range by key. To do that, use the `TKey` class, which is a sequence of key column values or their prefix. In the example below, the `"//tmp/table"` table is implied to have two key columns of types `string` and `int64` (or more than two, in which case a lexicographic comparison also works).

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .LowerLimit(TReadLimit().Key({"foo", 100}))
    .UpperLimit(TReadLimit().Key({"foo", 200})));
```

If you need string(s) with a specific key or index, you can use `TReadRange::Exact`:

```c++
auto path = TRichYPath("//tmp/table").AddRange(TReadRange()
    .Exact(TReadLimit().Key({"foo", 100})));
```

Vertical selection:

```c++
auto path = TRichYPath("//tmp/table").Columns({"a", "b"});
```

Renaming columns:

```c++
auto path = TRichYPath("//tmp/table").RenameColumns({{"a", "b"}, {"c", "d"}});
```

The table column *a* will display under the name *b*, and column *c* &mdash; as *d*. The function resets the full list of renamed columns, so calling `RenameColumns` again will erase the results of the previous one. `Columns` is applied after `RenameColumns`, meaning in this case *b* and *d* will be suitable for `Columns`, while *a* and *c* &mdash; are not suitable.

You can still use unnormalized paths, for example:

```c++
auto path = TRichYPath("//tmp/table[#0:#100500]");
```

### Format settings

There is a [TFormatHints](https://github.com/ytsaurus/YTsaurus/blob/main/mapreduce/yt/interface/client_method_options.h) class for fine-tuning formats. You can pass it in the `TTableReaderOptions::FormatHints` and `TTableWriterOptions::FormatHints` fields when creating a reader/writer.
If this setting works for the requested format, it is applied; otherwise, an exception is made.

Available settings:

- `SkipNullValuesForTNode` — do not create keys in the hash map for fields with the `#` value.
- [Type conversions](../../../user-guide/storage/data-types.md) when writing: `EnableStringToAllConversion`, `EnableAllToStringConversion`, `EnableIntegralTypeConversion%` (`uint64` <-> `int64`, enabled by default), `EnableIntegralToDoubleConversion`, `EnableTypeConversion` (all of the above simultaneously).

### Additional options

If you need even more precise read/write options, you can use the `TTableWriterOptions` and `TTableReaderOptions` structures. They contain the `Config` field, where additional settings can be passed in `TNode` format that correspond to the [table_writer](../../../user-guide/storage/io-configuration.md#table_writer) and [table_reader](../../../user-guide/storage/io-configuration.md#table_reader) parameters.

```c++
auto writer = client->CreateTableWriter<TNode>("//tmp/table",
    TTableWriterOptions().Config(TNode()("max_row_weight", 128 << 20)));
```

### Parallel read { #parallel_read }

There is a [library](https://github.com/ytsaurus/YTsaurus/blob/main/yt/cpp/mapreduce/library/parallel_io/parallel_reader.h) for parallel reading of tables.

## Running operations { #operations }

See [link](https://github.com/ytsaurus/ytsaurus/blob/main/yt/cpp/mapreduce/interface/operation.h).

Running an operation that contains a custom code requires:

- Inheriting from one of the interfaces (`IMapper`, `IReducer`).
- Defining the `Do()` function that should contain the record handler.
- Using one of the `REGISTER_*` macros.
- Calling the client/transaction method corresponding to the operation type in the client code.

The`IMapper` and`IReducer` interfaces accept the `TTableReader<input record type>` and `TTableWriter<output record type>` types as template parameters. The `Do()` function takes pointers to these types.

```c++
#include <mapreduce/yt/interface/operation.h>
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

Next, you need to create a macro that registers the custom class:

```c++
REGISTER_MAPPER(TExtractKeyMapper).
```

There is no need to generate unique IDs — the type_info mechanism is used for identification. The job type will be explicitly displayed within the operation web interface in the custom job start string:

```c++
./cppbinary --yt-map "TExtractKeyMapper" 1 0
```

If the user wants to ID their jobs some other way, they need to ensure the names are unique within the same application and use the `REGISTER_NAMED_*` macros:

```c++
REGISTER_NAMED_MAPPER("The best extract key mapper in the world", TExtractKeyMapper);
```

The functions for running individual operations are as follows:

Map:

-
   ```c++
   IOperationPtr Map(
       const TMapOperationSpec& spec,
       ::TIntrusivePtr<IMapperBase> mapper,
       const TOperationOptions& options = TOperationOptions())
   ```

Reduce:

-
   ```c++
   IOperationPtr Reduce(
       const TReduceOperationSpec& spec,
       ::TIntrusivePtr<IReducerBase> reducer,
       const TOperationOptions& options = TOperationOptions())
   ```

Join-Reduce:

-
   ```c++
   IOperationPtr JoinReduce(
       const TJoinReduceOperationSpec& spec,
       ::TIntrusivePtr<IReducerBase> reducer,
       const TOperationOptions& options = TOperationOptions())
   ```

 MapReduce:

-
   ```c++
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

You can specify a null pointer as `mapper`, which will make the operation skip the Map stage when running, and the function's second version will additionally start the [reduce_combiner](../../../user-guide/data-processing/operations/reduce.md#reduce_combiner) stage.

The operation start call parameters usually include:

- Structure with the required specification parameters.
- Pointers to instances of job classes.
- Additional options (`TOperationOptions`).

Be sure to add paths ([TRichYPath](#trichypath)) to the input and output tables. Seeing how in general (in particular, in the protobuf backend) the types of table records can be different, the `AddInput()` and `AddOutput()` functions of the specification are templates:

```c++
TMapOperationSpec spec;
spec.AddInput<TYaMRRow>("//tmp/table")
    .AddInput<TYaMRRow>("//tmp/other_table")
    .AddOutput<TNode>("//tmp/output_table");
```

Whenever possible, the discrepancy between the specification types and the types specified in IMapper/IReducer will be detected at runtime.

A call without additional parameters looks like this:

```c++
client->Map(spec, new TExtractKeyMapper);
```


This is safe because `TIntrusivePtr` is created internally for the job object.

### Transferring user files

The `TUserJobSpec` structure, which is part of the main specification, is used to transfer user files to the job's sandbox. File sets can vary for different job types in one operation:

```c++
TMapReduceOperationSpec spec;
spec.MapperSpec(TUserJobSpec().AddLocalFile("./file_for_mapper"))
    .ReducerSpec(TUserJobSpec().AddLocalFile("./file_for_reducer"));
```

To use files that are already in the system, call the `TUserJobSpec::AddFile(fileName)` method.

An important additional parameter is `TOperationOptions::Spec`. It is `TNode`, which can contain any specification parameters combined with the main specification generated by the client. Many of these options are described in the [Operation types](../../../user-guide/data-processing/operations/overview.md) section.

### Multiple input and output tables

If the operation writes to several output tables, the `AddRow()` method is used with two parameters:

```c++
writer->AddRow(row, tableIndex);
```

If the operation reads from multiple tables, the table index can be retrieved from `TTableReader` using the `GetTableIndex()` method.

### Protobuf backend

When using protobuf, it is possible to specify different record types for input and output tables. In this case, the declaration of the job's basic class uses `TTableReader` or `TTableWriter` not from a specific custom protobuf class, but from the basic `Message`:

```c++
class TProtoReducer
    : public IReducer<TTableReader<Message>, TTableWriter<Message>>
{ ... }
```

You should not write `AddInput/AddOutput()` in the operation's specification. You must provide a specific type.

In this case, the `GetRow()` and `AddRow()` methods become templates and must use specific user-defined types. For reading, you can select the function to call by using `GetTableIndex()` of the current entry before calling `GetRow()`. For writing, `AddRow(row, tableIndex)` is called. Types are controlled to be in accord with table indexes. 


### Initializing and serializing jobs

The `Start()` and `Finish()` methods are similar to their general interface counterparts, taking as a parameter the pointer to the corresponding `TTableWriter`.

```c++
virtual void Save(IOutputStream& stream) const;
virtual void Load(IInputStream& stream);
```

Serialization is not tied to any frameworks. The content of a serialized job is stored in the sandbox in the `jobstate` file. If not a single byte was written during `Save()`, the file will be missing. That said, `Load()` in the job will be called from an empty stream.

There is a `Y_SAVELOAD_JOB` helper that wraps the serialization from `util/ysaveload.h`.

The order of calling methods in a job is as follows: `default ctor; Load(); Start(); Do(); Finish()`.

### Preparing an operation from a job class { #prepare_operation }

The `IJob` class (the basic class for all jobs) has the virtual method `void PrepareOperation(const IOperationPreparationContext& context, TJobOperationPreparer& preparer) const` added, which can be overloaded in the user's jobs:

- `context` allows you to get information about input and output tables and, in particular, their schemas.
- `preparer` serves to control specification parameters.

You can set:
- Output table schema: `preparer.OutputSchema(tableIndex, schema)`.
- Message types for the protobuf input and output formats: `preparer.InputDescription<TYourMessage>(tableIndex)` and `preparer.OutputDescription<TYourMessage>(tableIndex)`.
- Column filtering for input tables: `preparer.InputColumnFilter(tableIndex, {"foo", "bar"})`.
- Ability to rename input table columns: `preparer.InputColumnRenaming(tableIndex, {{"foo", "not_foo"}})`. The renaming is applied *after* filtering.

All methods return `*this`, which enables chaining.

Usually, when using protobuf, specifying only `.OutputDescription` is sufficient, and the output table schema is inferred automatically.
If this is not the right behavior, you can set the second parameter (`inferSchema`) to `false`, such as `preparer.OutputDescription<TRow>(tableIndex, false)`.
For jobs marking `OutputDescription`, you can use short variants to start operations, such as `client->Map(new TMapper, input, output)`.

If there are several similar tables, their description can be combined into groups using the `.(Begin|End)(Input|Output)Group` methods.
`.Begin(Input|Output)Group` accepts the description of the table indexes that this group combines.
It can be either a `[begin, end)` pair or a container with `int`, for example:

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


### Notes

- The `IMapper::Do()` function takes all the records from the job as input. This is different from the general interface logic.
- The `IReducer::Do()` function accepts a range of records by one key, as before. In the case of the Reduce operation, this is determined by the `ReduceBy` specification parameter; in the case of JoinReduce, it is determined by `JoinBy`.
- If the job class is a template based on user-defined parameters, `REGISTER_*` must be called for each instance that needs to be run on the cluster.

## Getting information about operations and jobs

### GetOperation

Get [information about an operation](../../../api/commands.md#get_operation).

```c++
TOperationAttributes GetOperation(
    const TOperationId& operationId,
    const TGetOperationOptions& options)
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------------------------- | ----------------------------------- | ------- | ---------------------- |
| `operationId` |                                     |         | Operation ID. |
| `options` — optional settings: |                                     |         |                        |
| `AttributeFilter` | `TMaybe<TOperationAttributeFilter>` | none | Which attributes to return. |

### ListOperations

Get a list of operations [that match the filters](../../../api/commands.md#list_operation).

```c++
TListOperationsResult ListOperations(
    const TListOperationsOptions& options)
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------------------------- | -------------------------- | ------- | ------------------------------------------------------------ |
| `options` — optional settings: |                            |         |                                                              |
| `FromTime` | `TMaybe<TInstant>` | none | Start of the search ti*m*e interval. |
| `ToTime` | `TMaybe<TInstant>` | none | End of the search ti*m*e interval. |
| `CursorTime` | `TMaybe<TInstant>` | none | Cursor position (for pagination). |
| `CursorDirection` | `TMaybe<ECursorDirection>` | none | Which end of the ti*m*e interval to start listing operations from. |
| `Filter` | `TMaybe<TString>` | none | "Text factors" (in particular, title and paths to input/output tables) of the operation include `Filter` as a substring. |
| `Pool` | `TMaybe<TString>` | none | The operation was started in the `Pool` pool. |
| `User` | `TMaybe<TString>` | none | The operation was started by `User`. |
| `State` | `TMaybe<TString>` | none | The operation is in the `State` state. |
| `Type` | `TMaybe<EOperationType>` | none | The operation has the `Type` type. |
| `WithFailedJobs` | `TMaybe<bool>` | none | The operation has (`WithFailedJobs == true`) or does not have (`WithFailedJobs == false`) failed jobs |
| `IncludeArchive` | `TMaybe<bool>` | none | Whether to search for operations in the operations archive. |
| `IncludeCounters` | `TMaybe<bool>` | none | Whether to include statistics on the total number of operations (not just for the specified interval) in the response. |
| `Limit` | `TMaybe<i64>` | none | Return no more than `Limit` operations (the current maximum value of this parameter is `100`, which is also the default value). |

### UpdateOperationParameters

Update the runtime parameters of the [running operation](../../../api/commands.md#update_operation_parameters).

```c++
void UpdateOperationParameters(
    const TOperationId& operationId,
    const TUpdateOperationParametersOptions& options)
```

| **Parameter** | **Type** | Default value | **Description** |
| --------------------------------- | --------------------------------------- | ------- | ------------------------------------------------------------ |
| `operationId` | Operation ID |         |                                                              |
| `options` — optional settings: |                                         |         |                                                              |
| `Owner` | `TVector<TString>` | none | New operation owners. |
| `Pool` | `TMaybe<TString>` | none | Move the operation to the `Pool` pool (applies to all the pool trees the operation is running for). |
| `Weight` | `TMaybe<double>` | none | New operation weight (applies to all the pool trees the operation is running for). |
| `SchedulingOptionsPerPoolTree` | `TMaybe<TSchedulingOptionsPerPoolTree>` | none | Scheduler options for each pool tree. |

### GetJob

Get [information about the job](../../../api/commands.md#get_job).

```c++
TJobAttributes GetJob(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobOptions& options)
```

| **Parameter** | **Description** |
| ---------------------------------- | ----------- |
| `operationId` | Operation ID |
| `jobId` | Job ID |
| `options` — optional settings: |             |

### ListJobs

Get jobs that [match the specified filters](../../../api/commands.md#list_jobs).

```c++
TListJobsResult ListJobs(
    const TOperationId& operationId,
    const TListJobsOptions& options)
```

| **Parameter** | **Type** | **Default value** | **Description** |
| --------------------------------- | ----------------------------- | ------- | ------------------------------------------------------------ |
| `operationId` |                               |         | Operation ID. |
| `options` — optional settings: |                               |         |                                                              |
| `Type` | `TMaybe<EJobType>` | none | The job has the `Type` type. |
| `State` | `TMaybe<EJobState>` | none | The job is in the `State` state. |
| `Address` | `TMaybe<TString>` | none | The job is running (or was run) on the `Address` node. |
| `WithStderr` | `TMaybe<bool>` | none | Whether the job wrote anything to stderr. |
| `WithSpec` | `TMaybe<bool>` | none | Whether the job specification was saved. |
| `WithFailContext` | `TMaybe<bool>` | none | Whether the job's fail context was saved. |
| `SortField` | `TMaybe<EJobSortField>` | none | The field jobs are sorted by in the response. |
| `SortOrder` | `TMaybe<ESortOrder>` | none | Ascending or descending job sorting order. |
| `DataSource` | `TMaybe<EListJobsDataSource>` | none | Where to look for jobs: in the controller agent and Cypress (`Runtime`), in the archive of jobs (`Archive`), automatically depending on whether the operation is in Cypress (`Auto`), or other (`Manual`). |
| `IncludeCypress` | `TMaybe<bool>` | none | Whether to search for jobs in Cypress (the option factors into `DataSource == Manual`). |
| `IncludeControllerAgent` | `TMaybe<bool>` | none | Search for jobs in the agent controller (the option factors into `DataSource == Manual`). |
| `IncludeArchive` | `TMaybe<bool>` | none | Whether to search for jobs in the job archive (the option factors into `DataSource == Manual`). |
| `Limit` | `TMaybe<i64>` | none | Return no more than `Limit` of jobs. |
| `Offset` | `TMaybe<i64>` | none | Skip the first `Offset` jobs (in the sort order). |

### GetJobInput

Get the input of a [running or failed job](../../../api/commands.md#get_job_input).

```c++
IFileReaderPtr GetJobInput(
    const TJobId& jobId,
    const TGetJobInputOptions& options)
```

| **Parameter** | **Description** |
| ------------------------------------------- | -------- |
| `jobId` | Job ID. |
| `options` — optional settings (none yet) |          |

### GetJobFailContext

Get fail context of a [failed job](../../../api/commands.md#get_job_fail_context).

```c++
IFileReaderPtr GetJobFailContext(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobFailContextOptions& options)
```

| **Parameters** | **Description** |
| -------------------------------------------- | ----------- |
| `operationId` | Operation ID. |
| `jobId` | Job ID. |
| `options` — optional settings (none yet) |             |

### GetJobStderr

Get stderr of a [running or failed job](../../../api/commands.md#get_job_stderr).

```c++
IFileReaderPtr GetJobStderr(
    const TOperationId& operationId,
    const TJobId& jobId,
    const TGetJobStderrOptions& options)
```

| **Parameter** | **Description** |
| ------------------------------------------- | ----------- |
| `operationId` | Operation ID. |
| `jobId` | Job ID. |
| `options` — optional settings (none yet) |             |

## Working with file cache

Clusters have a common file cache that stores files by key, which is their MD5 hash. It is currently
at `//tmp/yt_wrapper/file_storage/new_cache`.
You can use the `PutFileToCache` and `GetFileFromCache` methods to work with it.
The same methods can be used to work with your cache that is located at a different path (but you will also need to clean it yourself).

{% note warning %}

All commands for working with the file cache are non-transactional, so they encompass the `IClient` methods, but not `IClientBase` or `ITransaction`.

{% endnote %}


### PutFileToCache

[Copy the file to the cache](../../../api/commands.md#put_file_to_cache). Returns a path to the cached file.

```c++
TYPath PutFileToCache(
        const TYPath& filePath,
        const TString& md5Signature,
        const TYPath& cachePath,
        const TPutFileToCacheOptions& options)
```

| **Parameter** | **Description** |
| ------------------------------------------- | ------------------------------------------------------------ |
| `filePath` | The path to the file in Cypress (the file must have the `md5` attribute; for this, it must be written with the `TFileWriterOptions::ComputeMD5` option). |
| `md5Signature` | The file's MD5 hash. |
| `cachePath` | The Cypress path to the file cache root. |
| `options` — optional settings (none yet) |                                                              |

### GetFileFromCache

[Get a path to the cached file](../../../api/commands.md#get_file_from_cache). Returns the path (or `Nothing`, if there is no file with this MD5).

```c++
TMaybe<TYPath> GetFileFromCache(
        const TString& md5Signature,
        const TYPath& cachePath,
        const TGetFileFromCacheOptions& options);
```

| **Parameter** | **Description** |
| ------------------------------------------- | -------------------------------------- |
| `md5Signature` | The file's MD5 hash. |
| `cachePath` | The Cypress path to the file cache root. |
| `options` — optional settings (none yet) |                                        |

## Environment variables

Currently these settings are available only from environment variables and are the same for all `IClient` instances. However, we are going to redesign this functionality so that almost everything can be configured directly from the code and changed at the individual `IClient` level.

| *Variable* | *Type* | *Value* |
| ------------------------------------- | ------- | ------------------------------------------------------------ |
| YT_TOKEN | string | Value of the custom token. |
| YT_TOKEN_PATH | string | Path to a file with the custom token. |
| YT_PREFIX | string | A path that is prefixed to all paths in Cypress. |
| YT_TRANSACTION | guid | External transaction ID. This parameter has `CreateClient()` create an `IClient` instance that runs in the context of the transaction. |
| YT_POOL | string | Pool for running user operations. |
| YT_FORCE_IPV4 | bool | Use only IPv4. |
| YT_FORCE_IPV6 | bool | Use only IPv6. |
| YT_CONTENT_ENCODING | string | HTTP header Content-Encoding: `identity`, `gzip`, `y-lzo`, `y-lzf` are supported. |
| YT_ACCEPT_ENCODING | string | HTTP header Accept-Encoding. |
| YT_USE_HOSTS | bool | Whether to use heavy proxies. |
| YT_HOSTS | string | Path in the heavy proxy list request. |
| YT_RETRY_COUNT | int | Number of attempts per HTTP request. |
| YT_START_OPERATION_RETRY_COUNT | int | The number of attempts to start the operation when the limit of simultaneously running operations is exceeded. |
| YT_VERSION | string | HTTP API version. |
| YT_SPEC | json | Specification added to each operation specification. Order: main specification, `YT_SPEC`, `TOperationOptions::Spec`. |
| YT_CONNECT_TIMEOUT | seconds | Socket connection timeout. |
| YT_SOCKET_TIMEOUT | seconds | Timeout for working the socket. |
| YT_TX_TIMEOUT | seconds | Transaction timeout. |
| YT_PING_INTERVAL | seconds | Interval between consecutive transaction pings. |
| YT_RETRY_INTERVAL | seconds | Interval between HTTP request attempts. |
| YT_RATE_LIMIT_EXCEEDED_RETRY_INTERVAL | seconds | The interval between attempts if the request rate limit is exceeded. |
| YT_START_OPERATION_RETRY_INTERVAL | seconds | Interval between attempts to start the operation. |

## Thread safety

`IClient` and `ITransaction` have no variable state. Thread-safe.

{% if audience == "internal" %}

## Unit tests

Unit tests for {{product-name}} operations are executed using UNITTEST or GTEST with the `mapreduce/yt/tests/yt_initialize_hook` hook added to PEERDIR. You likewise need to enable the `mapreduce/yt/tests/yt_unittest_lib` library, from which you need to use the `CreateTestClient()` method in tests instead of the regular `CreateClient()`, and also the `mapreduce/yt/python/recipe/recipe.inc` recipe, which creates a local {{product-name}} sandbox for the test client. If you need an operations archive for tests, use `mapreduce/yt/python/init_operations_archive/recipe/recipe194.inc` instead of the above-mentioned recipe.

Other than that, these are regular Arcadia unit tests.

Unfortunately, the local sandbox currently takes very long to run, so unit tests of several libraries are usually combined into one large test, whose size is set to `MEDIUM`.

Structure of the ya.make file for UNITTEST:

```
UNITTEST()

PEERDIR(
    mapreduce/yt/tests/yt_initialize_hook
    mapreduce/yt/tests/yt_unittest_lib
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
```

And for GTEST:

```
GTEST()

PEERDIR(
    mapreduce/yt/tests/yt_initialize_hook
    mapreduce/yt/tests/yt_unittest_lib
)

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/mapreduce/yt/python/recipe/recipe.inc)

END()
```

## Using old and new C++ API at the same time

The use of the old YaMR API and the new C++ API in one program is technically possible, but not recommended. A potential case where such combined use is justified would be ensuring a smooth transition of the program from one API to the other.

At the top of the program, both APIs must be initialized, for example this way:

```c++
int main(int argc, const char** argv) {
    NMR::Initialize(argc, argv);
    NYT::Initialize(argc, argv);
    ...
}
```

{% else %}{% endif %}
