---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/types/_includes/datatypes_primitive_string.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/types/_includes/datatypes_primitive_string.md
---
| Type | Description | Notes   |
----- | ----- | -----
| `String` | A string that can contain any binary data |
| `Utf8` | [UTF-8](https://en.wikipedia.org/wiki/UTF-8)-coded text. |
| `Json` | [JSON](https://en.wikipedia.org/wiki/JSON) represented as text | Doesn't support comparison |
| `JsonDocument` | [JSON](https://en.wikipedia.org/wiki/JSON) in an indexed binary representation | Doesn't support comparison |
| `Yson` | [YSON](../../udf/list/yson.md) in textual or binary presentation | Doesn't support comparison |
| `Uuid` | [UUID](https://tools.ietf.org/html/rfc4122) universal identifier | Not supported for table columns |

{% note info "Size limitations" %}

The maximum size of the value in a cell with any string data type is 8 MB.

{% endnote %}

Unlike the `JSON` data type that stores the original text representation passed by the user, `JsonDocument` uses an indexed binary representation. An important difference from the point of view of semantics is that `JsonDocument` doesn't preserve formatting, the order of keys in objects, or their duplicates.

Thanks to the indexed view, `JsonDocument` lets you bypass the document model using `JsonPath` without the need to parse the full content. This helps efficiently perform operations from the JSON API, reducing delays and the cost of user queries. Execution of `JsonDocument` queries can be up to several times more efficient depending on the type of load.

Due to the added redundancy, `JsonDocument` is less effective in storage. The additional storage overhead depends on the specific content, but is 20-30% of the original volume on average. Saving data in `JsonDocument` format requires additional conversion from the textual representation, which makes writing it less efficient. However, for most read-intensive scenarios that involve processing data from JSON, this data type is preferred and recommended.

{% note warning %}

[Double](https://en.wikipedia.org/wiki/Double-precision_floating-point_format) type is used to store (JSON Number) numerical values in `JsonDocument` and perform arithmetic operations over them in JSON API. Precision might be lost when non-standard representations of numbers are used in the source JSON document.

{% endnote %}