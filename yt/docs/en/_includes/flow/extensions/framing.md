# Framing library in {{product-name}} Flow

Flow framing helpers combine byte strings into blocks and split blocks back into byte strings. A byte string typically contains a serialized protobuf message. The [C++ wrappers]({{source-root}}/yt/yt/flow/extensions/framing/cpp) use [`library/cpp/framing`]({{source-root}}/library/cpp/framing). Use these helpers to agree on a data format between components and pack or unpack messages in C++.

A **frame** is a byte string wrapped in the selected format. `lenval` prefixes data with a varint length; `light_protoseq` uses a four-byte little-endian length; `protoseq` adds a sync marker after the data to the four-byte little-endian length. A **block** is a byte string containing one or more frames. A **batch** is a group of messages used to produce a block.

The shared helpers define the block format and processing rules. Connector configuration, defaults, compression, and unpacking-error policies belong to the connectors and are described on their pages.

## Choosing a format {#format}

The C++ helpers accept a `NYT::NFlow::EFramingFormat` value. See the [format reference](../../../flow/generated_docs/all_yson_structs.md#NYT_NFlow_EFramingFormat) for the byte layouts.

#|
|| **Format** | **C++: `EFramingFormat`** | **Value** ||
|| `auto` | `EFramingFormat::Auto` | `0` ||
|| `protoseq` | `EFramingFormat::Protoseq` | `1` ||
|| `lenval` | `EFramingFormat::Lenval` | `2` ||
|| `light_protoseq` | `EFramingFormat::LightProtoseq` | `3` ||
|#

Choose the format according to both ends of the exchange:

- Use `lenval` for a compact block and select `lenval` explicitly on the receiving side.
- Use `protoseq` when sync markers are needed to resume reading after partial corruption; select `protoseq` explicitly on the receiving side. `lenval` and `light_protoseq` cannot resynchronize after corruption.
- Select `light_protoseq` explicitly for both packing and unpacking: `auto` cannot detect it.
- Use `auto` for unpacking unknown data only when the sender can produce `protoseq` or `lenval`.

When packing, `auto` neither chooses a format dynamically nor preserves an original format: it always means `protoseq`. `PackBatch` and `PackBatches` with `EFramingFormat::Auto` therefore produce `protoseq`. This behavior does not define a connector's default.

{% note warning %}

Specify the format explicitly when it is known. In `auto` mode, the unpacker checks whether the block begins with a valid `protoseq` frame and otherwise selects `lenval`. A `lenval` block whose prefix also resembles `protoseq` can therefore be misinterpreted without an error.

{% endnote %}

## C++ API {#cpp-api}

`EFramingFormat` is declared in `yt/yt/flow/extensions/framing/cpp/public.h`; the functions are declared in `packer.h` and `unpacker.h` in the same directory. They all belong to `NYT::NFlow`.

### Packing one block {#cpp-pack-batch}

```cpp
std::string PackBatch(
    const std::vector<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat);
```

`PackBatch` reads `dataColumn` from each message, passes its bytes unchanged to the packer, preserves message order, and returns one block in `framingFormat`. Serialize protobuf messages before calling the helper. The column must exist. A non-null value must be a string; otherwise reading fails. A null value is read as an empty string and produces an empty frame.

### Splitting into multiple blocks {#cpp-pack-batches}

```cpp
std::vector<std::pair<std::string, std::vector<TOutputMessageConstPtr>>> PackBatches(
    const std::deque<TOutputMessageConstPtr>& messages,
    const std::string& dataColumn,
    const EFramingFormat framingFormat,
    const i32 maxRowsPerMessage,
    const i32 maxBytesPerMessage);
```

`PackBatches` returns pairs of a packed block and the original messages in its batch. It adds a message to the current batch and then finishes the batch if either condition holds:

- The message count reaches `maxRowsPerMessage`.
- The sum of `TOutputMessage::ByteSize` reaches `maxBytesPerMessage`.

The final incomplete batch is also packed. Empty input does not produce an empty batch.

`TOutputMessage::ByteSize` estimates the memory used by the whole Flow message, including metadata and every payload column. It is not limited to `dataColumn`. `maxBytesPerMessage` is a threshold for the sum of these estimates, not a strict encoded-block size limit. The check occurs after adding a message, so the final message can make the sum exceed the threshold. Framing overhead is excluded from this sum.

### Unpacking {#cpp-unpack-batch}

```cpp
template <typename TProto>
TErrorOr<std::vector<TProto>> UnpackBatch(
    TStringBuf rawData Y_LIFETIME_BOUND,
    const EFramingFormat framingFormat);
```

`UnpackBatch<TProto>` parses frames from `rawData` into `TProto` messages, preserving order on success. If protobuf parsing fails, or the unpacker skips bytes between frames or at the end of the block, it returns an error `TErrorOr`. Skipped-data errors have the `is_permanent=true` attribute. Parsed messages are not returned as partial success.

Use `UnpackBatch<TStringBuf>` to retrieve frame contents without parsing protobuf. This mode checks framing structure and the absence of skipped bytes only. It returns arbitrary contents of a syntactically valid frame without protobuf validation.

The elements returned by `UnpackBatch<TStringBuf>` are views into `rawData`, not copies. Keep the input buffer alive and unchanged while using them. `Y_LIFETIME_BOUND` helps the compiler diagnose some lifetime errors but does not extend the buffer's lifetime. In contrast, `UnpackBatch<TProto>` returns independent protobuf objects.

Framing contains no checksum, so no `UnpackBatch` mode checks one. If corrupted data forms a syntactically valid frame, `TStringBuf` mode returns it as is; protobuf mode additionally requires it to parse as a valid `TProto` message.
