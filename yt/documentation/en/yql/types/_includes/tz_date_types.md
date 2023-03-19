---
vcsPath: ydb/docs/ru/core/yql/reference/yql-core/types/_includes/tz_date_types.md
sourcePath: ydb/docs/ru/core/yql/reference/yql-core/types/_includes/tz_date_types.md
---
### Supporting types with a time zone label

Time zone label for the `TzDate`, `TzDatetime`, `TzTimestamp` types is an attribute that is used:

* During ([CAST](../../syntax/expressions.md#cast), [DateTime::Parse](../../udf/list/datetime.md#parse), [DateTime::Format](../../udf/list/datetime.md#format)) transformation into a string and from a string.
* In [DateTime::Split](../../udf/list/datetime.md#split), the time zone component appears in `Resource<TM>`.

The point in time for these types is stored in UTC, and the timezone label doesn't participate in any other calculations in any way. For example:
```yql
select --these expressions are always true for any timezones:  the timezone doesn't affect the point in time.
    AddTimezone(CurrentUtcDate(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDate(), "America/New_York"),
    AddTimezone(CurrentUtcDatetime(), "Europe/Moscow") ==
        AddTimezone(CurrentUtcDatetime(), "America/New_York");
```
Keep in mind that when converting between `TzDate` and `TzDatetime`, or `TzTimestamp` the date's midnight doesn't follow the local time zone, but midnight in UTC for the date in UTC.
