---
vcsPath: yql/docs_yfm/docs/ru/yql-product/udf/list/protobuf.md
sourcePath: yql-product/udf/list/protobuf.md
---
# Protobuf UDF
Converting protobuf messages to YQL structures

Function list:

```Protobuf::Parse(String?{Flags:AutoMap}) -> <Struct>?``` -- Unpackages the message to a YQL structure
```Protobuf::TryParse(String?{Flags:AutoMap}) -> <Struct>?``` -- Safely unpackages the message. Returns Null in case of an error
```Protobuf::Serialize(<Struct>?{Flags:AutoMap}) -> String``` -- Serializes the structure into a protobuf string

Before using, create metadata similar to what's written in the `_yql_proto_field` meta attribute.

## Supported descriptor fields {#yql_proto_field_desc}
* `name`: the name of the protobuf message. It is comprised of the package name and message name separated by the dot: "package.message", where:
   * package stores the protofile
   * message is used to collect data
* `meta` is the descriptor of the protobuf message (compressed zlib, base64)
* `format` is the protobuf serialization format. Possible values: `protobin` (default) — binary, `prototext` — text, `json`
* `skip` — the number of bytes in each value to be ignored before parsing (0 by default)
* `view` — the dictionary defining protobuf message view settings. The following settings are supported:
   * `enum` — protobuf enumeration view in YQL. Possible values: `number` (default), `name`, `full_name`
   * `recursion` — a method to process recursive messages. Possible values: `fail` (default) — return an error, `ignore` — skip recursive inserts, `bytes` — convert recursive inserts into byte strings
   * `yt_mode` — `{{product-name}}the Mode` (`false` by default) used to process {{product-name}}protobuf-specific options. If the message lacks `NYT.default_field_flags` or its value is not `SERIALIZATION_YT`, the message is converted to a byte string. If its value is `SERIALIZATION_YT`, the message is processed as usual (recursive message processing is defined by the `recursion` option)
* `lists` — the dictionary that defines view settings for repeated fields in protobuf messages. The following settings are supported:
   * `optional` — displays repeated fields as optional lists instead of regular lists (`true` by default)

{% note warning "Attention!" %}

If `lists` `optional`=`false`, we don't recommend writing the `Protobuf::Parse` result into tables because the table scheme becomes incompatible if new repeated fields are introduced.

{% endnote %}


Examples:

```yql
USE Hahn;

$config = @@{
    "name": "ru.yandex.metrika.wv2.proto.BufferWrapper",
    "meta": "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"
}@@;

$try_parse = Udf(Protobuf::TryParse, $config As TypeConfig);

SELECT $try_parse(data) AS proto, $try_parse('BAD ROW') AS bad_row
FROM `//home/yql/protobuf_example`;
```

<!--- [Example in the Parse web interface](https://cluster-name.yql/Operations/W64e3Da9vIEIfHUFBPytZeXuud362mbq8ewxUm4HJqU=)
- [Example in the TryParse web interface](https://cluster-name.yql/Operations/W8uTomUObwCuZFY_91u2i9L64dpCg9Ji73HwTeNIOfs=)-->
