#include "meters.h"

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NIOTest {

using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

TLatencyMeter::TLatencyMeter()
    : Last_(GetCpuInstant())
{ }

TDuration TLatencyMeter::Tick()
{
    auto last = Last_;
    Last_ = GetCpuInstant();
    return CpuDurationToDuration(Last_ - last);
}

TDuration TLatencyMeter::GetElapsed()
{
    return CpuDurationToDuration(GetCpuInstant() - Last_);
}

////////////////////////////////////////////////////////////////////////////////

TRusageMeter::TRusageMeter(ERusageWho who)
    : Who_(who)
    , Last_(GetRusage(Who_))
{ }

TRusage TRusageMeter::Tick()
{
    auto last = Last_;
    Last_ = GetRusage(Who_);
    return Last_ - last;
}

////////////////////////////////////////////////////////////////////////////////

TProcessRusageMeter::TProcessRusageMeter(TProcessId processId)
    : ProcessId_(processId)
    , Last_(GetProcessRusage(ProcessId_))
{ }

TRusage TProcessRusageMeter::Tick()
{
    auto current = GetProcessRusage(ProcessId_);
    if (current.UserTime < Last_.UserTime || current.SystemTime < Last_.SystemTime) {
        return {};
    }
    auto last = Last_;
    Last_ = current;
    return Last_ - last;
}

////////////////////////////////////////////////////////////////////////////////

TCumulativeRusageMeter::TCumulativeRusageMeter(std::vector<TProcessRusageMeter> processMeters)
    : ProcessMeters_(std::move(processMeters))
{ }

TRusage TCumulativeRusageMeter::Tick()
{
    TRusage result;
    for (auto& meter : ProcessMeters_) {
        result += meter.Tick();
    }
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NIOTest
