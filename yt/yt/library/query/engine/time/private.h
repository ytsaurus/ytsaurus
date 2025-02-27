#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

struct TDay
{
    ui16 DayOfTheYear:10; // [1; 366]
    ui8 Month:6; // [1; 12]
    i8 DayOfTheMonth; // [1; 31]
    i8 Year; // Year minus 1970, [0; 80)

    TDay(i8 dayOfTheMonth,
        i16 dayOfTheYear,
        i8 month,
        i8 year)
            : DayOfTheYear(dayOfTheYear)
            , Month(month)
            , DayOfTheMonth(dayOfTheMonth)
            , Year(year)
    {
        Y_ASSERT(dayOfTheYear >= 1 && dayOfTheYear <= 366);
        Y_ASSERT(month >= 1 && month <= 12);
        Y_ASSERT(DayOfTheMonth >= 1 && DayOfTheMonth <= 31);
        Y_ASSERT(Year >= 0 && Year < 80);
    }
};

static_assert(sizeof(TDay) == 4, "TDay is not packed");

struct TTimestampedDay
{
    i8 DayOfTheWeek; // [0; 6]
    i8 DayOfTheMonth; // [1; 31]
    i16 DayOfTheYear; // [1; 366]
    i8 Month; // [1; 12]
    i8 Year; // Year minus 1970, [0; 80)
    i64 Timestamp; // Unix timestamp
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient::NRoutines
