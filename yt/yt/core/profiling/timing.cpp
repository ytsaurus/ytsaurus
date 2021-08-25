#include "timing.h"

#include <yt/yt/core/misc/assert.h>
#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/serialize.h>

#include <util/system/hp_timer.h>

#include <array>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////

namespace {

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

} // namespace

////////////////////////////////////////////////////////////////////////////////

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

TValue DurationToValue(TDuration duration)
{
    return duration.MicroSeconds();
}

TDuration ValueToDuration(TValue value)
{
    // TDuration is unsigned and thus does not support negative values.
    if (value < 0) {
        value = 0;
    }
    return TDuration::MicroSeconds(static_cast<ui64>(value));
}

TValue CpuDurationToValue(TCpuDuration cpuDuration)
{
    return cpuDuration > 0
        ? DurationToValue(CpuDurationToDuration(cpuDuration))
        : -DurationToValue(CpuDurationToDuration(-cpuDuration));
}

////////////////////////////////////////////////////////////////////////////////

TWallTimer::TWallTimer(bool start)
{
    if (start) {
        Start();
    }
}

TInstant TWallTimer::GetStartTime() const
{
    return CpuInstantToInstant(GetStartCpuTime());
}

TDuration TWallTimer::GetElapsedTime() const
{
    return CpuDurationToDuration(GetElapsedCpuTime());
}

TCpuInstant TWallTimer::GetStartCpuTime() const
{
    return StartTime_;
}

TCpuDuration TWallTimer::GetElapsedCpuTime() const
{
    return Duration_ + GetCurrentDuration();
}

TValue TWallTimer::GetElapsedValue() const
{
    return DurationToValue(GetElapsedTime());
}

void TWallTimer::Start()
{
    StartTime_ = GetCpuInstant();
    Active_ = true;
}

void TWallTimer::StartIfNotActive()
{
    if (!Active_) {
        Start();
    }
}

void TWallTimer::Stop()
{
    Duration_ += GetCurrentDuration();
    StartTime_ = 0;
    Active_ = false;
}

void TWallTimer::Restart()
{
    Duration_ = 0;
    Start();
}

TCpuDuration TWallTimer::GetCurrentDuration() const
{
    return Active_
        ? Max<TCpuDuration>(GetCpuInstant() - StartTime_, 0)
        : 0;
}

void TWallTimer::Persist(const TStreamPersistenceContext& context)
{
    using NYT::Persist;

    Persist(context, Active_);
    if (context.IsSave()) {
        auto duration = GetElapsedCpuTime();
        Persist(context, duration);
    } else {
        Persist(context, Duration_);
        StartTime_ = Active_ ? GetCpuInstant() : 0;
    }
}

////////////////////////////////////////////////////////////////////////////////

TFiberWallTimer::TFiberWallTimer()
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Start(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
