#include <yt/yt/library/query/misc/udf_cpp_abi.h>

using namespace NYT::NQueryClient::NUdf;

extern "C" void FormatTimestamp(
    bool localtime,
    TExpressionContext* context,
    char** result,
    int* resultLen,
    int64_t timestamp,
    char* format,
    int formatLen);

extern "C" void format_timestamp(
    TExpressionContext* context,
    char** result,
    int* resultLen,
    int64_t timestamp,
    char* format,
    int formatLen)
{
    FormatTimestamp(/*localtime*/ false, context, result, resultLen, timestamp, format, formatLen);
}

extern "C" void format_timestamp_localtime(
    TExpressionContext* context,
    char** result,
    int* resultLen,
    int64_t timestamp,
    char* format,
    int formatLen)
{
    FormatTimestamp(/*localtime*/ true, context, result, resultLen, timestamp, format, formatLen);
}

extern "C" int64_t TimestampFloorHour(int64_t);
extern "C" int64_t TimestampFloorDay(int64_t);
extern "C" int64_t TimestampFloorWeek(int64_t);
extern "C" int64_t TimestampFloorMonth(int64_t);
extern "C" int64_t TimestampFloorQuarter(int64_t);
extern "C" int64_t TimestampFloorYear(int64_t);

extern "C" int64_t timestamp_floor_hour(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorHour(timestamp);
}
extern "C" int64_t timestamp_floor_day(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorDay(timestamp);
}
extern "C" int64_t timestamp_floor_week(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorWeek(timestamp);
}
extern "C" int64_t timestamp_floor_month(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorMonth(timestamp);
}
extern "C" int64_t timestamp_floor_quarter(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorQuarter(timestamp);
}
extern "C" int64_t timestamp_floor_year(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorYear(timestamp);
}

extern "C" int64_t TimestampFloorHourLocaltime(int64_t);
extern "C" int64_t TimestampFloorDayLocaltime(int64_t);
extern "C" int64_t TimestampFloorWeekLocaltime(int64_t);
extern "C" int64_t TimestampFloorMonthLocaltime(int64_t);
extern "C" int64_t TimestampFloorQuarterLocaltime(int64_t);
extern "C" int64_t TimestampFloorYearLocaltime(int64_t);

extern "C" int64_t timestamp_floor_hour_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorHourLocaltime(timestamp);
}
extern "C" int64_t timestamp_floor_day_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorDayLocaltime(timestamp);
}
extern "C" int64_t timestamp_floor_week_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorWeekLocaltime(timestamp);
}
extern "C" int64_t timestamp_floor_month_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorMonthLocaltime(timestamp);
}
extern "C" int64_t timestamp_floor_quarter_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorQuarterLocaltime(timestamp);
}
extern "C" int64_t timestamp_floor_year_localtime(TExpressionContext*, int64_t timestamp)
{
    return TimestampFloorYearLocaltime(timestamp);
}
