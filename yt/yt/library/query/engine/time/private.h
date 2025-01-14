#pragma once

#include <util/system/types.h>

namespace NYT::NQueryClient::NRoutines {

////////////////////////////////////////////////////////////////////////////////

struct TDay
{
    i8 DayOfTheWeek; // [0; 6]
    i8 DayOfTheMonth; // [1; 31]
    i16 DayOfTheYear; // [1; 366]
    i8 Month; // [1; 12]
    i16 Year; // Year minus 1970, [0; 80)
};

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
