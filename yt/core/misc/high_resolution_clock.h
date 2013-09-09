#pragma once

#include "common.h"

namespace NYT {
namespace NClock {

// copied from qualiy/misc/HPTimer.h
////////////////////////////////////////////////////////////////////////////////

typedef i64 STime;
double GetSeconds(const STime &a);
// получить текущее время
void GetTime(STime *pTime);
// получить время, прошедшее с момента, записанного в *pTime, при этом в *pTime будет записано текущее время
double GetTimePassed(STime *pTime);
// получить частоту процессора
double GetClockRate();

////////////////////////////////////////////////////////////////////////////////

} // namespace NHPTime
} // namespace NYT
