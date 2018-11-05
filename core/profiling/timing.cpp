#include "timing.h"
#include "profiler.h"

#include <util/system/hp_timer.h>
#include <util/system/sanitizers.h>

#include <util/generic/singleton.h>

#include <array>

namespace NYT {
namespace NProfiling  {

////////////////////////////////////////////////////////////////////////////////

static const auto CalibrationInterval = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TClockConverter
{
public:
    static TClockConverter* Get()
    {
        return Singleton<TClockConverter>();
    }

    // TDuration is unsigned and does not support negative values,
    // thus we consider two cases separately.
    TInstant Convert(TCpuInstant cpuInstant)
    {
        auto state = GetCalibrationState();
        return cpuInstant >= state.CpuInstant
            ? state.Instant + CpuDurationToDuration(cpuInstant - state.CpuInstant)
            : state.Instant - CpuDurationToDuration(state.CpuInstant - cpuInstant);
    }

    TCpuInstant Convert(TInstant instant)
    {
        auto state = GetCalibrationState();
        return instant >= state.Instant
            ? state.CpuInstant + DurationToCpuDuration(instant - state.Instant)
            : state.CpuInstant - DurationToCpuDuration(state.Instant - instant);
    }

private:
    struct TCalibrationState
    {
        TCpuInstant CpuInstant;
        TInstant Instant;
    };

    std::atomic<TCpuInstant> NextCalibrationCpuInstant_{0};
    TSpinLock CalibrationLock_;
    std::array<TCalibrationState, 2> CalibrationStates_;
    std::atomic<ui32> CalibrationStateIndex_{0};


    Y_DECLARE_SINGLETON_FRIEND();

    TClockConverter()
    {
        Calibrate(0);
    }

    void CalibrateIfNeeded()
    {
        auto nowCpuInstant = GetCpuInstant();
        if (nowCpuInstant < NextCalibrationCpuInstant_) {
            return;
        }

        TTryGuard<TSpinLock> guard(CalibrationLock_);
        if (!guard.WasAcquired()) {
            return;
        }

        Calibrate(1 - CalibrationStateIndex_);
        NextCalibrationCpuInstant_ += DurationToCpuDuration(CalibrationInterval);
    }

    void Calibrate(int index)
    {
        auto& state = CalibrationStates_[index];
        state.CpuInstant = GetCpuInstant();
        state.Instant = TInstant::Now();
        CalibrationStateIndex_ = index;
    }

    TCalibrationState GetCalibrationState()
    {
        CalibrateIfNeeded();
        if (NSan::TSanIsOn()) {
            // The data structure is designed to avoid locking on read
            // but we cannot explain it to TSan.
            TGuard<TSpinLock> guard(CalibrationLock_);
            return CalibrationStates_[CalibrationStateIndex_];
        } else {
            return CalibrationStates_[CalibrationStateIndex_];
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TInstant GetInstant()
{
    return CpuInstantToInstant(GetCpuInstant());
}

TDuration CpuDurationToDuration(TCpuDuration duration)
{
    // TDuration is unsigned and thus does not support negative values.
    Y_ASSERT(duration >= 0);
    return TDuration::Seconds(static_cast<double>(duration) / NHPTimer::GetClockRate());
}

TCpuDuration DurationToCpuDuration(TDuration duration)
{
    return static_cast<TCpuDuration>(static_cast<double>(duration.MicroSeconds()) * NHPTimer::GetClockRate() / 1000000);
}

TInstant CpuInstantToInstant(TCpuInstant instant)
{
    return TClockConverter::Get()->Convert(instant);
}

TCpuInstant InstantToCpuInstant(TInstant instant)
{
    return TClockConverter::Get()->Convert(instant);
}

TValue DurationToValue(TDuration duration)
{
    return duration.MicroSeconds();
}

TDuration ValueToDuration(TValue value)
{
    // TDuration is unsigned and thus does not support negative values.
    Y_ASSERT(value >= 0);
    return TDuration::MicroSeconds(static_cast<ui64>(value));
}

TValue CpuDurationToValue(TCpuDuration duration)
{
    return duration > 0
        ? DurationToValue(CpuDurationToDuration(duration))
        : -DurationToValue(CpuDurationToDuration(-duration));
}

////////////////////////////////////////////////////////////////////////////////

TWallTimer::TWallTimer()
{
    Restart();
}

TInstant TWallTimer::GetStartTime() const
{
    return CpuInstantToInstant(StartTime_);
}

TDuration TWallTimer::GetElapsedTime() const
{
    return CpuDurationToDuration(Duration_ + GetCurrentDuration());
}

TValue TWallTimer::GetElapsedValue() const
{
    return DurationToValue(GetElapsedTime());
}

void TWallTimer::Start()
{
    StartTime_ = GetCpuInstant();
}

void TWallTimer::Stop()
{
    Duration_ += GetCurrentDuration();
    StartTime_ = 0;
}

void TWallTimer::Restart()
{
    Duration_ = 0;
    Start();
}

TCpuDuration TWallTimer::GetCurrentDuration() const
{
    return Max<TCpuDuration>(GetCpuInstant() - StartTime_, 0);
}

////////////////////////////////////////////////////////////////////////////////

TCpuTimer::TCpuTimer()
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Start(); })
{ }

////////////////////////////////////////////////////////////////////////////////

TWallTimingGuard::TWallTimingGuard(TDuration* value)
    : Value_(value)
{ }

TWallTimingGuard::~TWallTimingGuard()
{
    *Value_ += TWallTimer::GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

TCpuTimingGuard::TCpuTimingGuard(TDuration* value)
    : Value_(value)
{ }

TCpuTimingGuard::~TCpuTimingGuard()
{
    *Value_ += TCpuTimer::GetElapsedTime();
}

////////////////////////////////////////////////////////////////////////////////

TProfilingTimingGuard::TProfilingTimingGuard(const TProfiler& profiler, TMonotonicCounter* counter)
    : Profiler_(profiler)
    , Counter_(counter)
    , StartInstant_(GetCpuInstant())
{ }

TProfilingTimingGuard::~TProfilingTimingGuard()
{
    auto duration = CpuDurationToDuration(GetCpuInstant() - StartInstant_);
    Profiler_.Increment(*Counter_, duration.MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
