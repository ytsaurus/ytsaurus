#include "stdafx.h"
#include "timing.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/high_resolution_clock.h>

namespace NYT {
namespace NProfiling  {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger SILENT_UNUSED Logger("Profiling");
static const TDuration CalibrationInterval = TDuration::Seconds(3);

////////////////////////////////////////////////////////////////////////////////

class TClockConverter
{
public:
    TClockConverter()
        : NextCalibrationCpuInstant(0)
    { }

    TInstant Convert(TCpuInstant instant)
    {
        CalibrateIfNeeded();
        // TDuration is unsigned and thus does not support negative values.
        return
            instant >= CalibrationCpuInstant
            ? CalibrationInstant + CpuDurationToDuration(instant - CalibrationCpuInstant)
            : CalibrationInstant - CpuDurationToDuration(CalibrationCpuInstant - instant);
    }

    TCpuInstant Convert(TInstant instant)
    {
        CalibrateIfNeeded();
        // TDuration is unsigned and thus does not support negative values.
        return
            instant >= CalibrationInstant
            ? CalibrationCpuInstant + DurationToCpuDuration(instant - CalibrationInstant)
            : CalibrationCpuInstant - DurationToCpuDuration(CalibrationInstant - instant);
    }

private:
    void CalibrateIfNeeded()
    {
        auto nowClock = GetCpuInstant();
        if (nowClock > NextCalibrationCpuInstant) {
            Calibrate();
        }
    }

    void Calibrate()
    {
        auto nowCpuInstant = GetCpuInstant();
        auto nowInstant = TInstant::Now();
        // Beware of local time readjustments!
        if (NextCalibrationCpuInstant != 0 && nowCpuInstant >= CalibrationCpuInstant) {
            auto expected = (CalibrationInstant + CpuDurationToDuration(nowCpuInstant - CalibrationCpuInstant)).MicroSeconds();
            auto actual = nowInstant.MicroSeconds();
            LOG_DEBUG("Clock recalibrated (Diff: %" PRId64 ")", expected - actual);
        }
        CalibrationCpuInstant = nowCpuInstant;
        CalibrationInstant = nowInstant;
        NextCalibrationCpuInstant = nowCpuInstant + DurationToCpuDuration(CalibrationInterval);
    }

    TInstant CalibrationInstant;
    TCpuInstant CalibrationCpuInstant;
    TCpuInstant NextCalibrationCpuInstant;

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
