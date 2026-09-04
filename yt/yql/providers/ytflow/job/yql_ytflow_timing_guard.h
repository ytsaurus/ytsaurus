#pragma once

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/core/profiling/timing.h>

#include <util/datetime/base.h>

#include <optional>


namespace NYql::NYtflow {

class TCpuVCpuTimeCounter
{
public:
    inline TCpuVCpuTimeCounter(
        NYT::NProfiling::TTimeCounter& cpuTimeCounter,
        NYT::NProfiling::TTimeCounter& vcpuTimeCounter,
        std::optional<double> cpuToVCpuFactor);

    inline void Add(TDuration delta);

private:
    NYT::NProfiling::TTimeCounter* CpuTimeCounter = nullptr;
    NYT::NProfiling::TTimeCounter* VCpuTimeCounter = nullptr;
    std::optional<double> CpuToVCpuFactor;
};


template <typename TTimer>
class TCounterIncrementingTimingGuard
{
public:
    TCounterIncrementingTimingGuard(TCpuVCpuTimeCounter& timeCounter);

    ~TCounterIncrementingTimingGuard();

private:
    TCpuVCpuTimeCounter* TimeCounter = nullptr;

    TTimer Timer;
};

using TSimpleTimingGuard = TCounterIncrementingTimingGuard<NYT::NProfiling::TWallTimer>;


} // namespace NYql::NYtflow

#include "yql_ytflow_timing_guard-inl.h"
