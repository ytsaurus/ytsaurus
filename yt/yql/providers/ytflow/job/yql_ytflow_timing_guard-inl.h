#pragma once

#include "yql_ytflow_timing_guard.h"

#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NYtflow {

TCpuVCpuTimeCounter::TCpuVCpuTimeCounter(
    NYT::NProfiling::TTimeCounter& cpuTimeCounter,
    NYT::NProfiling::TTimeCounter& vcpuTimeCounter,
    std::optional<double> cpuToVCpuFactor)
    : CpuTimeCounter(&cpuTimeCounter)
    , VCpuTimeCounter(&vcpuTimeCounter)
    , CpuToVCpuFactor(cpuToVCpuFactor)
{ }

void TCpuVCpuTimeCounter::Add(TDuration delta)
{
    CpuTimeCounter->Add(delta);

    if (CpuToVCpuFactor) {
        VCpuTimeCounter->Add(delta * *CpuToVCpuFactor);
    }
}

template <typename TTimer>
TCounterIncrementingTimingGuard<TTimer>::TCounterIncrementingTimingGuard(
    TCpuVCpuTimeCounter& timeCounter)
    : TimeCounter(&timeCounter)
{
    Timer.Start();
}

template <typename TTimer>
TCounterIncrementingTimingGuard<TTimer>::~TCounterIncrementingTimingGuard()
{
    Timer.Stop();

    TimeCounter->Add(Timer.GetElapsedTime());
}

} // namespace NYql::NYtflow
