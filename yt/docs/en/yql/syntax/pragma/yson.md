# Yson

Management of Yson UDF default behavior. To learn more, see the [documentation](../../udf/list/yson.md), in particular, [Yson::Options](../../udf/list/yson.md#ysonoptions).

The Yson pragma affects all subsequent expressions up to the end of the module in which it occurs.

## yson.AutoConvert

| Value type | By default |
| --- | --- |
| Flag | false |

Automatic conversion of values to the required data type in all Yson UDF calls, including implicit calls.

## yson.Strict

| Value type | By default |
| --- | --- |
| Flag | true |

Strict mode control in all Yson UDF calls, including implicit calls. Empty or `"true"` value enables strict mode. If the value is `"false"`, strict mode is disabled.

## yson.DisableStrict

| Value type | By default |
| --- | --- |
| Flag | false |

An inverted version of `yson.Strict`. Empty or `"true"` value disables strict mode. If the value is `"false"`, strict mode is enabled.