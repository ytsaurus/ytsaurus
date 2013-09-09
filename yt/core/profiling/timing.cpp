#include "stdafx.h"
#include "timing.h"

#include <core/misc/high_resolution_clock.h>

namespace NYT {
namespace NProfiling  {

////////////////////////////////////////////////////////////////////////////////

static const TDuration CalibrationInterval = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TClockConverter
{
public:
    TClockConverter()
        : NextCalibrationCpuInstant(0)
        , CalibrationLock(false)
    {
        Calibrate(0);
    }

    // TDuration is unsigned and does not support negative values,
    // thus we consider two cases separately.
    
    TInstant Convert(TCpuInstant cpuInstant)
    {
        auto state = GetCalibrationState();
        return
            cpuInstant >= state.CpuInstant
            ? state.Instant + CpuDurationToDuration(cpuInstant - state.CpuInstant)
            : state.Instant - CpuDurationToDuration(state.CpuInstant - cpuInstant);
    }

    TCpuInstant Convert(TInstant instant)
    {
        auto state = GetCalibrationState();
        return
            instant >= state.Instant
            ? state.CpuInstant + DurationToCpuDuration(instant - state.Instant)
            : state.CpuInstant - DurationToCpuDuration(state.Instant - instant);
    }

private:
    struct TCalibrationState
    {
        TCpuInstant CpuInstant;
        TInstant Instant;
    };

    TCpuInstant NextCalibrationCpuInstant;
    TAtomic CalibrationLock;
    TCalibrationState CalibrationStates[2];
    TAtomic CalibrationStateIndex;


    void CalibrateIfNeeded()
    {
        auto nowCpuInstant = GetCpuInstant();
        if (nowCpuInstant > NextCalibrationCpuInstant) {
            if (AtomicCas(&CalibrationLock, true, false)) {
                Calibrate(1 - CalibrationStateIndex);
                NextCalibrationCpuInstant += DurationToCpuDuration(CalibrationInterval);
                AtomicSet(CalibrationLock, false);
            }
        }
    }
    
    void Calibrate(int index)
    {
        auto& state = CalibrationStates[index];
        state.CpuInstant = GetCpuInstant();
        state.Instant = TInstant::Now();
        AtomicSet(CalibrationStateIndex, index);
    }

    TCalibrationState GetCalibrationState()
    {
        CalibrateIfNeeded();
        return CalibrationStates[CalibrationStateIndex];
    }


};

static TClockConverter ClockConverter;

////////////////////////////////////////////////////////////////////////////////

TCpuInstant GetCpuInstant()
{
    return GetCycleCount();
}

TDuration CpuDurationToDuration(TCpuDuration duration)
{
    // TDuration is unsigned and thus does not support negative values.
    YASSERT(duration >= 0);
    // TODO(babenko): get rid of this dependency on NHPTimer
    return TDuration::Seconds((double) duration / NClock::GetClockRate());
}

TCpuDuration DurationToCpuDuration(TDuration duration)
{
    // TODO(babenko): get rid of this dependency on NHPTimer
    return (double) duration.MicroSeconds() * NClock::GetClockRate() / 1000000;
}

TInstant CpuInstantToInstant(TCpuInstant instant)
{
    return ClockConverter.Convert(instant);
}

TCpuInstant InstantToCpuInstant(TInstant instant)
{
    return ClockConverter.Convert(instant);
}

TValue DurationToValue(TDuration duration)
{
    return duration.MicroSeconds();
}

TValue CpuDurationToValue(TCpuDuration duration)
{
    return
        duration > 0
        ? DurationToValue(CpuDurationToDuration(duration))
        : -DurationToValue(CpuDurationToDuration(-duration));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NProfiling
} // namespace NYT
