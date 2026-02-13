#pragma once

#include <yt/yt/library/profiling/sensor.h>

#include <util/system/types.h>

namespace NYT::NQueueAgent {

////////////////////////////////////////////////////////////////////////////////

class TPassProfiler final
{
public:
    TPassProfiler(const NProfiling::TProfiler& profiler);

    void OnStart(i64 index, TInstant startTime) const;
    void OnFinish(TDuration duration) const;
    void OnError() const;

private:
    NProfiling::TGauge Index_;
    NProfiling::TGauge StartTime_;
    NProfiling::TEventTimer Duration_;
    NProfiling::TCounter Errors_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
