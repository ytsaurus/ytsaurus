#include "pass_profiler.h"

namespace NYT::NQueueAgent {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TPassProfiler::TPassProfiler(const TProfiler& profiler)
{
    auto passProfiler = profiler.WithPrefix("/pass");

    Index_ = passProfiler.Gauge("/index");
    StartTime_ = passProfiler.Gauge("/start_time");
    Duration_ = passProfiler.Timer("/duration");
    Errors_ = passProfiler.Counter("/errors");
}

void TPassProfiler::OnStart(i64 index, TInstant startTime) const
{
    Index_.Update(index);
    StartTime_.Update(startTime.SecondsFloat());
}

void TPassProfiler::OnError() const
{
    Errors_.Increment();
}

void TPassProfiler::OnFinish(TDuration duration) const
{
    Duration_.Record(duration);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueueAgent
