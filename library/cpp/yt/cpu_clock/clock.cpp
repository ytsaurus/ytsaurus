#include "clock.h"

#include <util/system/hp_timer.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TCpuInstant GetCpuInstant()
{
    unsigned hi, lo;
    __asm__ __volatile__("rdtsc"
    : "=a"(lo), "=d"(hi));
    return static_cast<unsigned long long>(lo) | (static_cast<unsigned long long>(hi) << 32);
}

// Re-calibrate every 1B CPU ticks.
constexpr auto CalibrationCpuPeriod = 1'000'000'000;

struct TCalibrationState
{
    TCpuInstant CpuInstant;
    TInstant Instant;
    double TicksToMicroseconds;
    double MicrosecondsToTicks;
};

double GetMicrosecondsToTicks()
{
    return static_cast<double>(NHPTimer::GetCyclesPerSecond()) / 1'000'000;
}

double GetTicksToMicroseconds()
{
    return 1.0 / GetMicrosecondsToTicks();
}

TCalibrationState GetCalibrationState(TCpuInstant cpuInstant)
{
    thread_local TCalibrationState State;

    if (State.CpuInstant + CalibrationCpuPeriod < cpuInstant) {
        State.CpuInstant = cpuInstant;
        State.Instant = TInstant::Now();
        State.TicksToMicroseconds = GetTicksToMicroseconds();
        State.MicrosecondsToTicks = GetMicrosecondsToTicks();
    }

    return State;
}

TCalibrationState GetCalibrationState()
{
    return GetCalibrationState(GetCpuInstant());
}

TDuration CpuDurationToDuration(TCpuDuration cpuDuration, double ticksToMicroseconds)
{
    // TDuration is unsigned and thus does not support negative values.
    if (cpuDuration < 0) {
        return TDuration::Zero();
    }
    return TDuration::MicroSeconds(static_cast<ui64>(cpuDuration * ticksToMicroseconds));
}

TCpuDuration DurationToCpuDuration(TDuration duration, double microsecondsToTicks)
{
    return static_cast<TCpuDuration>(duration.MicroSeconds() * microsecondsToTicks);
}

TInstant GetInstant()
{
    auto cpuInstant = GetCpuInstant();
    auto state = GetCalibrationState(cpuInstant);
    YT_ASSERT(cpuInstant >= state.CpuInstant);
    return state.Instant + CpuDurationToDuration(cpuInstant - state.CpuInstant, state.TicksToMicroseconds);
}

TDuration CpuDurationToDuration(TCpuDuration cpuDuration)
{
    return CpuDurationToDuration(cpuDuration, GetTicksToMicroseconds());
}

TCpuDuration DurationToCpuDuration(TDuration duration)
{
    return DurationToCpuDuration(duration, GetMicrosecondsToTicks());
}

TInstant CpuInstantToInstant(TCpuInstant cpuInstant)
{
    // TDuration is unsigned and does not support negative values,
    // thus we consider two cases separately.
    auto state = GetCalibrationState();
    return cpuInstant >= state.CpuInstant
        ? state.Instant + CpuDurationToDuration(cpuInstant - state.CpuInstant, state.TicksToMicroseconds)
        : state.Instant - CpuDurationToDuration(state.CpuInstant - cpuInstant, state.TicksToMicroseconds);
}

TCpuInstant InstantToCpuInstant(TInstant instant)
{
    // See above.
    auto state = GetCalibrationState();
    return instant >= state.Instant
        ? state.CpuInstant + DurationToCpuDuration(instant - state.Instant, state.MicrosecondsToTicks)
        : state.CpuInstant - DurationToCpuDuration(state.Instant - instant, state.MicrosecondsToTicks);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
