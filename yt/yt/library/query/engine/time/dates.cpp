#include "dates_lut_localtime.h"
#include "dates_lut_utc.h"
#include "dates.h"

#include <library/cpp/yt/error/error.h>

#include <yt/yt/library/numeric/algorithm_helpers.h>


namespace NYT::NQueryClient::NRoutines {

using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

constexpr int MaxFormatLength = 30;
constexpr int BufferLength = 128;

////////////////////////////////////////////////////////////////////////////////

void ValidateTimestamp(i64 timestamp, bool localtime)
{
    auto minTimestamp = localtime ? MinLocaltimeTimestamp : MinUtcTimestamp;
    THROW_ERROR_EXCEPTION_IF(timestamp < minTimestamp,
        "Timestamp is smaller than minimal value: got %v < %v ",
        timestamp,
        minTimestamp);

    auto maxTimestamp = localtime ? MaxLocaltimeTimestamp : MaxUtcTimestamp;
    THROW_ERROR_EXCEPTION_IF(timestamp > maxTimestamp,
        "Timestamp is greater than maximal value: got %v > %v ",
        timestamp,
        maxTimestamp);
}

void ValidateFormatStringLength(int length)
{
    THROW_ERROR_EXCEPTION_IF(length > MaxFormatLength,
        "Format string is too long: %v > %v",
        length,
        MaxFormatLength);
}

bool IsLutApplicable(i64 timestamp, bool localtime)
{
    return
        timestamp >= (localtime ? MinLutLocaltimeTimestamp : MinLutUtcTimestamp) &&
        timestamp < (localtime ? MaxLutLocaltimeTimestamp : MaxLutUtcTimestamp);
}

////////////////////////////////////////////////////////////////////////////////

void FormatTimestamp(
    bool localtime,
    TExpressionContext* context,
    char** result,
    int* resultLength,
    i64 timestamp,
    char* format,
    int formatLength)
{
    ValidateFormatStringLength(formatLength);
    ValidateTimestamp(timestamp, localtime);

    struct tm timeinfo;
    if (localtime) {
        localtime_r(&timestamp, &timeinfo);
    } else {
        gmtime_r(&timestamp, &timeinfo);
    }

    char buffer[MaxFormatLength + 1];
    memcpy(buffer, format, formatLength);
    buffer[formatLength] = '\0';

    auto* resultPtr = context->AllocateUnaligned(BufferLength, NWebAssembly::EAddressSpace::WebAssembly);
    auto length = strftime(resultPtr, BufferLength, buffer, &timeinfo);

    *result = resultPtr;
    *resultLength = length;
}

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorWeekViaLib(i64 timestamp)
{
    struct tm timeinfo;
    gmtime_r(&timestamp, &timeinfo);

    int daysFromMonday = (timeinfo.tm_wday + 6) % 7;

    timestamp -= daysFromMonday * 86400;
    timestamp -= timeinfo.tm_hour * 60 * 60;
    timestamp -= timeinfo.tm_min * 60;
    timestamp -= timeinfo.tm_sec;

    return timestamp;
}

i64 TimestampFloorMonthViaLib(i64 timestamp)
{
    struct tm timeinfo;
    gmtime_r(&timestamp, &timeinfo);

    timestamp -= (timeinfo.tm_mday - 1) * 24 * 60 * 60;
    timestamp -= timeinfo.tm_hour * 60 * 60;
    timestamp -= timeinfo.tm_min * 60;
    timestamp -= timeinfo.tm_sec;

    return timestamp;
}

i64 TimestampFloorQuarterViaLib(i64 timestamp)
{
    struct tm timeinfo;
    gmtime_r(&timestamp, &timeinfo);

    timestamp -= (timeinfo.tm_mday - 1) * 24 * 60 * 60;
    timestamp -= timeinfo.tm_hour * 60 * 60;
    timestamp -= timeinfo.tm_min * 60;
    timestamp -= timeinfo.tm_sec;

    while (timeinfo.tm_mon % 4 != 0) {
        timestamp--;

        gmtime_r(&timestamp, &timeinfo);

        timestamp -= (timeinfo.tm_mday - 1) * 24 * 60 * 60;
        timestamp -= timeinfo.tm_hour * 60 * 60;
        timestamp -= timeinfo.tm_min * 60;
        timestamp -= timeinfo.tm_sec;
    }

    return timestamp;
}

i64 TimestampFloorYearViaLib(i64 timestamp)
{
    struct tm timeinfo;
    gmtime_r(&timestamp, &timeinfo);

    timestamp -= timeinfo.tm_yday * 24 * 60 * 60;
    timestamp -= timeinfo.tm_hour * 60 * 60;
    timestamp -= timeinfo.tm_min * 60;
    timestamp -= timeinfo.tm_sec;

    return timestamp;
}

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorWeekViaLut(i64 timestamp)
{
    const auto& day = UtcLut[FindUtc(timestamp)];

    timestamp -= timestamp % 86400;
    timestamp -= day.DayOfTheWeek * 86400;
    return timestamp;
}

i64 TimestampFloorMonthViaLut(i64 timestamp)
{
    const auto& day = UtcLut[FindUtc(timestamp)];

    timestamp -= timestamp % 86400;
    timestamp -= (day.DayOfTheMonth - 1) * 86400;

    return timestamp;
}

i64 TimestampFloorQuarterViaLut(i64 timestamp)
{
    const auto* day = &UtcLut[FindUtc(timestamp)];

    timestamp -= timestamp % 86400;
    timestamp -= (day->DayOfTheMonth - 1) * 86400;

    while (day->Month % 4 != 1) {
        timestamp--;

        day = &UtcLut[FindUtc(timestamp)];
        timestamp -= timestamp % 86400;
        timestamp -= (day->DayOfTheMonth - 1) * 86400;
    }

    return timestamp;
}

i64 TimestampFloorYearViaLut(i64 timestamp)
{
    const auto& day = UtcLut[FindUtc(timestamp)];

    timestamp -= timestamp % 86400;
    timestamp -= (day.DayOfTheYear - 1) * 86400;

    return timestamp;
}

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorDayLocaltimeViaLut(i64 timestamp)
{
    return LocaltimeLut[FindLocal(timestamp)].Timestamp;
}

i64 TimestampFloorWeekLocaltimeViaLut(i64 timestamp)
{
    const auto* day = &LocaltimeLut[FindLocal(timestamp)];
    day -= day->DayOfTheWeek;
    return day->Timestamp;
}

i64 TimestampFloorMonthLocaltimeViaLut(i64 timestamp)
{
    const auto* day = &LocaltimeLut[FindLocal(timestamp)];
    day -= day->DayOfTheMonth - 1;
    return day->Timestamp;
}

i64 TimestampFloorQuarterLocaltimeViaLut(i64 timestamp)
{
    const auto* day = &LocaltimeLut[FindLocal(timestamp)];
    day -= day->DayOfTheMonth - 1;
    while (day->Month % 4 != 1) {
        day -= 1;
        day -= day->DayOfTheMonth - 1;
    }
    return day->Timestamp;
}

i64 TimestampFloorYearLocaltimeViaLut(i64 timestamp)
{
    const auto* day = &LocaltimeLut[FindLocal(timestamp)];
    day = day - (day->DayOfTheYear - 1);
    return day->Timestamp;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
i64 BinarySearchTm(i64 timestamp, i64 leftShift, T callback)
{
    tm timeinfo;
    localtime_r(&timestamp, &timeinfo);

    return NYT::BinarySearch(timestamp - leftShift, timestamp, [&] (i64 probe) {
        tm probeTimeinfo;
        localtime_r(&probe, &probeTimeinfo);
        return !callback(probeTimeinfo, timeinfo);
    });
}

i64 TimestampFloorDayLocaltimeViaLib(i64 timestamp)
{
    constexpr i64 TwoDays = 60 * 60 * 24 * 2;

    return BinarySearchTm(timestamp, TwoDays, [] (const tm& probe, const tm& anchor) {
        return probe.tm_yday == anchor.tm_yday;
    });
}

i64 TimestampFloorWeekLocaltimeViaLib(i64 timestamp)
{
    timestamp = TimestampFloorDayLocaltimeViaLib(timestamp);

    tm timeinfo;
    localtime_r(&timestamp, &timeinfo);

    while (timeinfo.tm_wday != 1) {
        timestamp--;
        timestamp = TimestampFloorDayLocaltimeViaLib(timestamp);
        localtime_r(&timestamp, &timeinfo);
    }

    return timestamp;
}

i64 TimestampFloorMonthLocaltimeViaLib(i64 timestamp)
{
    constexpr i64 TwoMonths = 60 * 60 * 24 * 31 * 2;

    return BinarySearchTm(timestamp, TwoMonths, [] (const tm& probe, const tm& anchor) {
        return probe.tm_mon == anchor.tm_mon;
    });
}

i64 TimestampFloorQuarterLocaltimeViaLib(i64 timestamp)
{
    constexpr i64 FiveMonths = 60 * 60 * 24 * 31 * 5;

    return BinarySearchTm(timestamp, FiveMonths, [] (const tm& probe, const tm& anchor) {
        return probe.tm_mon - probe.tm_mon % 4 == anchor.tm_mon - anchor.tm_mon % 4;
    });
}

i64 TimestampFloorYearLocaltimeViaLib(i64 timestamp)
{
    constexpr i64 TwoYears = 60 * 60 * 24 * 366 * 2;

    return BinarySearchTm(timestamp, TwoYears, [] (const tm& probe, const tm& anchor) {
        return probe.tm_year == anchor.tm_year;
    });
}

////////////////////////////////////////////////////////////////////////////////

i64 TimestampFloorHour(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    return timestamp - timestamp % 3600;
}

i64 TimestampFloorDay(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    return timestamp - timestamp % 86400;
}

i64 TimestampFloorWeek(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    if (IsLutApplicable(timestamp, /*localtime*/ false)) {
        return TimestampFloorWeekViaLut(timestamp);
    } else {
        return TimestampFloorWeekViaLib(timestamp);
    }
}

i64 TimestampFloorMonth(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    if (IsLutApplicable(timestamp, /*localtime*/ false)) {
        return TimestampFloorMonthViaLut(timestamp);
    } else {
        return TimestampFloorMonthViaLib(timestamp);
    }
}

i64 TimestampFloorQuarter(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    if (IsLutApplicable(timestamp, /*localtime*/ false)) {
        return TimestampFloorQuarterViaLut(timestamp);
    } else {
        return TimestampFloorQuarterViaLib(timestamp);
    }
}

i64 TimestampFloorYear(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ false);

    if (IsLutApplicable(timestamp, /*localtime*/ false)) {
        return TimestampFloorYearViaLut(timestamp);
    } else {
        return TimestampFloorYearViaLib(timestamp);
    }
}

i64 TimestampFloorHourLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    return timestamp - timestamp % 3600;;
}

i64 TimestampFloorDayLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    if (IsLutApplicable(timestamp, /*localtime*/ true)) {
        return TimestampFloorDayLocaltimeViaLut(timestamp);
    } else {
        return TimestampFloorDayLocaltimeViaLib(timestamp);
    }
}

i64 TimestampFloorWeekLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    if (IsLutApplicable(timestamp, /*localtime*/ true)) {
        return TimestampFloorWeekLocaltimeViaLut(timestamp);
    } else {
        return TimestampFloorWeekLocaltimeViaLib(timestamp);
    }
}

i64 TimestampFloorMonthLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    if (IsLutApplicable(timestamp, /*localtime*/ true)) {
        return TimestampFloorMonthLocaltimeViaLut(timestamp);
    } else {
        return TimestampFloorMonthLocaltimeViaLib(timestamp);
    }
}

i64 TimestampFloorQuarterLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    if (IsLutApplicable(timestamp, /*localtime*/ true)) {
        return TimestampFloorQuarterLocaltimeViaLut(timestamp);
    } else {
        return TimestampFloorQuarterLocaltimeViaLib(timestamp);
    }
}

i64 TimestampFloorYearLocaltime(i64 timestamp)
{
    ValidateTimestamp(timestamp, /*localtime*/ true);

    if (IsLutApplicable(timestamp, /*localtime*/ true)) {
        return TimestampFloorYearLocaltimeViaLut(timestamp);
    } else {
        return TimestampFloorYearLocaltimeViaLib(timestamp);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
