---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/pragma/yson.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/syntax/_includes/pragma/yson.md
---
## Yson

Management of Yson UDF default behavior. To learn more, see [documentation](../../../udf/list/yson.md), in particular, [Yson::Options](../../../udf/list/yson.md#ysonoptions).

### `yson.AutoConvert`

| Value type | By default |
| --- | --- |
| Flag | false |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

### `yson.Strict`

| Value type | By default |
| --- | --- |
| Flag | true |

Strict mode control in all Yson UDF calls, including implicit calls. If the value is omitted or is `"true"`, it enables the strict mode. If the value is `"false"`, it disables the strict mode.

### `yson.DisableStrict`

| Value type | By default |
| --- | --- |
| Flag | false |

An inverted version of `yson.Strict`. If the value is omitted or is `"true"`, it disables the strict mode. If the value is `"false"`, it enables the strict mode.
