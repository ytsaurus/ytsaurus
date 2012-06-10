#include "stdafx.h"
#include "high_resolution_clock.h"

#include <util/datetime/cputimer.h>

namespace NYT {

using namespace NClock;

////////////////////////////////////////////////////////////////////////////////////////////////////

static double fProcFreq1 = 1;

////////////////////////////////////////////////////////////////////////////////////////////////////

double NClock::GetSeconds( const NClock::STime &a )
{
    return (static_cast<double>(a)) * fProcFreq1;
}

double NClock::GetClockRate()
{
    return 1. / fProcFreq1;
}

void NClock::GetTime(STime *pTime)
{
    *pTime = GetCycleCount();
}

double NClock::GetTimePassed( STime *pTime )
{
    STime old(*pTime);
    GetTime(pTime);
    return GetSeconds(*pTime - old);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
static double EstimateCPUClock()
{
    ui64 startCycle = 0;
    ui64 startMS = 0;
    for (;;) {
        startMS = MicroSeconds();
        startCycle = GetCycleCount();
        ui64 n = MicroSeconds();
        if (n - startMS < 100) {
            break;
        }
    }
    Sleep(TDuration::MicroSeconds(5000));
    ui64 finishCycle = 0;
    ui64 finishMS = 0;
    for (;;) {
        finishMS = MicroSeconds();
        if (finishMS - startMS < 100)
            continue;
        finishCycle = GetCycleCount();
        ui64 n = MicroSeconds();
        if (n - finishMS < 100) {
            break;
        }
    }
    return (finishCycle - startCycle) * 1000000.0 / (finishMS - startMS);
}
////////////////////////////////////////////////////////////////////////////////////////////////////

static void InitHPTimer()
{
    const size_t N_VEC = 9;
    double vec[N_VEC];
    for (unsigned i = 0; i < N_VEC; ++i)
        vec[i] = EstimateCPUClock();
    std::sort(vec, vec + N_VEC);

    fProcFreq1 = 1 / vec[N_VEC / 2];
    //printf("freq = %g\n", 1 / fProcFreq1 / 1000000);
}

////////////////////////////////////////////////////////////////////////////////////////////////////
// это вспомогательная структура для автоматической инициализации HP timer'а
struct SHPTimerInit
{
    SHPTimerInit() { InitHPTimer(); }
};
static SHPTimerInit hptInit;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
