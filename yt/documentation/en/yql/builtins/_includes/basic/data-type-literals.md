---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/data-type-literals.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/builtins/_includes/basic/data-type-literals.md
---
## Literals of primitive types {#data-type-literals}

For primitive types, you can create literals based on string literals.

**Syntax**

`<Primitive type>( <string>[, <additional attributes>] )`

Unlike `CAST("myString" AS MyType)`:

* The check for literal's castability to the desired type occurs at validation.
* The result is non-optional.

For the `Date`, `Datetime`, `Timestamp`, and `Interval` data types, literals are supported only in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). `Interval` has the following differences from the standard:

* It supports the negative sign for shifts to the past.
* Microseconds can be expressed as fractional parts of seconds.
* You can't use units of measurement exceeding one week.
* The options with the beginning/end of the interval and with repetitions, are not supported.

For the `TzDate`, `TzDatetime`, `TzTimestamp` data types, literals are also set in the format conforming to [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601), but instead of the optional Z suffix, they specify the [IANA name of the time zone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones), separated by a comma (for example, GMT or Europe/Moscow).

{% include [decimal args](../../../_includes/decimal_args.md) %}

**Examples**
```yql
SELECT
  Bool("true"),
  Uint8("0"),
  Int32("-1"),
  Uint32("2"),
  Int64("-3"),
  Uint64("4"),
  Float("-5"),
  Double("6"),
  Decimal("1.23", 5, 2), -- до 5 десятичных знаков, из которых 2 после запятой
  String("foo"),
  Utf8("привет"),
  Yson("<a=1>[3;%false]"),
  Json(@@{"a":1,"b":null}@@),
  Date("2017-11-27"),
  Datetime("2017-11-27T13:24:00Z"),
  Timestamp("2017-11-27T13:24:00.123456Z"),
  Interval("P1DT2H3M4.567890S"),
  TzDate("2017-11-27,Europe/Moscow"),
  TzDatetime("2017-11-27T13:24:00,America/Los_Angeles"),
  TzTimestamp("2017-11-27T13:24:00.123456,GMT"),
  Uuid("f9d5cc3f-f1dc-4d9c-b97e-766e57ca4ccb");
```
