#pragma once

#include <yt/yt/library/query/engine_api/expression_context.h>
#include <yt/yt/library/query/engine_api/position_independent_value.h>

#include <util/system/types.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

// For "0001-01-01T00:00:00Z".
constexpr i64 MinUtcTimestamp = -62135596800;
// For "9999-12-31T23:59:59.999999999Z".
constexpr i64 MaxUtcTimestamp = 253402300799;

// For "0001-01-01T00:00:00".
constexpr i64 MinLocaltimeTimestamp = -62135605817;
// For "9999-12-31T23:59:59".
constexpr i64 MaxLocaltimeTimestamp = 253402289999;

constexpr i64 MinLutUtcTimestamp = 0;
constexpr i64 MaxLutUtcTimestamp = 2524608000;

constexpr i64 MinLutLocaltimeTimestamp = -10800;
constexpr i64 MaxLutLocaltimeTimestamp = 2524597200;

////////////////////////////////////////////////////////////////////////////////

void FormatTimestamp(
    bool localtime,
    TExpressionContext* context,
    char** result,
    int* resultLen,
    i64 timestamp,
    char* format,
    int formatLen);

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorHour(i64 timestamp);
i64 TimestampFloorDay(i64 timestamp);
i64 TimestampFloorWeek(i64 timestamp);
i64 TimestampFloorMonth(i64 timestamp);
i64 TimestampFloorQuarter(i64 timestamp);
i64 TimestampFloorYear(i64 timestamp);

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorHourLocaltime(i64 timestamp);
i64 TimestampFloorDayLocaltime(i64 timestamp);
i64 TimestampFloorWeekLocaltime(i64 timestamp);
i64 TimestampFloorMonthLocaltime(i64 timestamp);
i64 TimestampFloorQuarterLocaltime(i64 timestamp);
i64 TimestampFloorYearLocaltime(i64 timestamp);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
