#include "stdafx.h"
#include "timing.h"

#include <ytlib/logging/log.h>
#include <ytlib/misc/high_resolution_clock.h>

namespace NYT {
namespace NProfiling  {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Profiling");

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

private:
    static const TDuration CalibrationInterval;

    void CalibrateIfNeeded()
    {
        auto nowClock = GetCpuInstant();
        if (nowClock > NextCalibrationCpuInstant) {
            Calibrate();
        }
    }

    void Calibrate()
    {
        auto cpuNow = GetCpuInstant();
        auto nowInstant = TInstant::Now();
        if (NextCalibrationCpuInstant != 0) {
            auto expected = (CalibrationInstant + CpuDurationToDuration(cpuNow - CalibrationCpuInstant)).MicroSeconds();
            auto actual = nowInstant.MicroSeconds();
            LOG_DEBUG("Clock recalibrated (Diff: %" PRId64 ")", expected - actual);
        }
        CalibrationCpuInstant = cpuNow;
        CalibrationInstant = nowInstant;
        NextCalibrationCpuInstant = cpuNow + DurationToCpuDuration(CalibrationInterval);
    }

    TInstant CalibrationInstant;
    TCpuInstant CalibrationCpuInstant;
    TCpuInstant NextCalibrationCpuInstant;

};

const TDuration TClockConverter::CalibrationInterval = TDuration::Seconds(3);

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
    static TClockConverter converter;
    return converter.Convert(instant);
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
