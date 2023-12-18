#pragma once

#include "rusage.h"

#include <yt/yt/core/profiling/public.h>

namespace NYT::NIOTest {

////////////////////////////////////////////////////////////////////////////////

class TLatencyMeter
{
public:
    TLatencyMeter();

    TDuration Tick();
    TDuration GetElapsed();

private:
    NProfiling::TCpuInstant Last_;
};

////////////////////////////////////////////////////////////////////////////////

class TRusageMeter
{
public:
    explicit TRusageMeter(ERusageWho who);

    TRusage Tick();

private:
    ERusageWho Who_;
    TRusage Last_;
};

////////////////////////////////////////////////////////////////////////////////

class TProcessRusageMeter
{
public:
    explicit TProcessRusageMeter(TProcessId processId);

    TRusage Tick();

private:
    TProcessId ProcessId_;
    TRusage Last_;
};

////////////////////////////////////////////////////////////////////////////////

class TCumulativeRusageMeter
{
public:
    explicit TCumulativeRusageMeter(std::vector<TProcessRusageMeter> processMeters);

    TRusage Tick();

private:
    std::vector<TProcessRusageMeter> ProcessMeters_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
