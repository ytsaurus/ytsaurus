#include "spin_wait_slow_path_logger.h"
#include "private.h"

#include <library/cpp/yt/cpu_clock/clock.h>

#include <library/cpp/yt/threading/spin_wait_hook.h>

namespace NYT::NThreading {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::atomic<bool> SpinLockSlowPathLoggingHookRegistered;
std::atomic<TCpuDuration> SpinWaitSlowPathLoggingThreshold;

const auto& Logger = ThreadingLogger;

void SpinWaitSlowPathLoggingHook(
    TCpuDuration cpuDelay,
    const ::TSourceLocation& location,
    ESpinLockActivityKind activityKind)
{
    if (cpuDelay >= SpinWaitSlowPathLoggingThreshold) {
        YT_LOG_DEBUG("Spin wait took too long (SourceLocation: %, ActivityKind: %v, Delay: %v)",
            location.File ? Format("%v:%v", location.File, location.Line) : "<unknown>",
            activityKind,
            CpuDurationToDuration(cpuDelay));
    }
}

} // namespace

void SetSpinWaitSlowPathLoggingThreshold(TDuration threshold)
{
    SpinWaitSlowPathLoggingThreshold.store(DurationToCpuDuration(threshold));
    if (!SpinLockSlowPathLoggingHookRegistered.exchange(true)) {
        RegisterSpinWaitSlowPathHook(&SpinWaitSlowPathLoggingHook);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NThreading
