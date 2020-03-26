#include "timing.h"

#include <yt/core/misc/singleton.h>

#include <util/system/sanitizers.h>
#include <util/system/spinlock.h>

#include <array>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

static const auto CalibrationInterval = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TClockConverter
{
public:
    static TClockConverter* Get()
    {
        return LeakySingleton<TClockConverter>();
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

    double GetClockRate()
    {
        return ClockRate_;
    }

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()

    const double ClockRate_;

    struct TCalibrationState
    {
        TCpuInstant CpuInstant;
        TInstant Instant;
    };

    std::atomic<TCpuInstant> NextCalibrationCpuInstant_ = {0};
    TSpinLock CalibrationLock_;
    std::array<TCalibrationState, 2> CalibrationStates_;
    std::atomic<size_t> CalibrationStateIndex_ = {0};


    TClockConverter()
        : ClockRate_(EstimateClockRate())
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

    static double EstimateClockRateOnce()
    {
        ui64 startCycle = 0;
        ui64 startMS = 0;

        for (;;) {
            startMS = MicroSeconds();
            startCycle = GetCpuInstant();

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

            if (finishMS - startMS < 100) {
                continue;
            }

            finishCycle = GetCpuInstant();

            ui64 n = MicroSeconds();

            if (n - finishMS < 100) {
                break;
            }
        }

        return (finishCycle - startCycle) * 1000000.0 / (finishMS - startMS);
    }

    static double EstimateClockRate()
    {
        const size_t N = 9;
        std::array<double, N> estimates;

        for (auto& estimate : estimates) {
            estimate = EstimateClockRateOnce();
        }

        std::sort(estimates.begin(), estimates.end());

        return estimates[N / 2];
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
    if (duration < 0) {
        duration = 0;
    }
    return TDuration::Seconds(static_cast<double>(duration) / TClockConverter::Get()->GetClockRate());
}

TCpuDuration DurationToCpuDuration(TDuration duration)
{
    return static_cast<TCpuDuration>(static_cast<double>(duration.MicroSeconds()) * TClockConverter::Get()->GetClockRate() / 1000000);
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
    if (value < 0) {
        value = 0;
    }
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

////////////////////////////////////////////////////////////////////////////////

TFiberWallTimer::TFiberWallTimer()
    : NConcurrency::TContextSwitchGuard(
        [this] () noexcept { Stop(); },
        [this] () noexcept { Start(); })
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
