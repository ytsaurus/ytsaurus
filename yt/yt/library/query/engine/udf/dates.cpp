#include <yt/yt/library/query/misc/udf_cpp_abi.h>

#include <util/system/types.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void FormatTimestamp(
    bool localtime,
    TExpressionContext* context,
    char** result,
    int* resultLen,
    i64 timestamp,
    char* format,
    int formatLen);

extern "C" void format_timestamp(
    TExpressionContext* context,
    char** result,
    int* resultLen,
    i64 timestamp,
    char* format,
    int formatLen)
{
    FormatTimestamp(/*localtime*/ false, context, result, resultLen, timestamp, format, formatLen);
}

extern "C" void format_timestamp_localtime(
    TExpressionContext* context,
    char** result,
    int* resultLen,
    i64 timestamp,
    char* format,
    int formatLen)
{
    FormatTimestamp(/*localtime*/ true, context, result, resultLen, timestamp, format, formatLen);
}

extern "C" i64 TimestampFloorHour(i64);
extern "C" i64 TimestampFloorDay(i64);
extern "C" i64 TimestampFloorWeek(i64);
extern "C" i64 TimestampFloorMonth(i64);
extern "C" i64 TimestampFloorQuarter(i64);
extern "C" i64 TimestampFloorYear(i64);

extern "C" i64 timestamp_floor_hour(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorHour(timestamp);
}
extern "C" i64 timestamp_floor_day(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorDay(timestamp);
}
extern "C" i64 timestamp_floor_week(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorWeek(timestamp);
}
extern "C" i64 timestamp_floor_month(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorMonth(timestamp);
}
extern "C" i64 timestamp_floor_quarter(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorQuarter(timestamp);
}
extern "C" i64 timestamp_floor_year(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorYear(timestamp);
}

extern "C" i64 TimestampFloorHourLocaltime(i64);
extern "C" i64 TimestampFloorDayLocaltime(i64);
extern "C" i64 TimestampFloorWeekLocaltime(i64);
extern "C" i64 TimestampFloorMonthLocaltime(i64);
extern "C" i64 TimestampFloorQuarterLocaltime(i64);
extern "C" i64 TimestampFloorYearLocaltime(i64);

extern "C" i64 timestamp_floor_hour_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorHourLocaltime(timestamp);
}
extern "C" i64 timestamp_floor_day_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorDayLocaltime(timestamp);
}
extern "C" i64 timestamp_floor_week_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorWeekLocaltime(timestamp);
}
extern "C" i64 timestamp_floor_month_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorMonthLocaltime(timestamp);
}
extern "C" i64 timestamp_floor_quarter_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorQuarterLocaltime(timestamp);
}
extern "C" i64 timestamp_floor_year_localtime(TExpressionContext*, i64 timestamp)
{
    return TimestampFloorYearLocaltime(timestamp);
}
