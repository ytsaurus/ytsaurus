#include "instance_limits_tracker.h"
#include "instance.h"
#include "private.h"

#include <yt/core/concurrency/periodic_executor.h>

namespace NYT::NContainers {

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = ContainersLogger;

////////////////////////////////////////////////////////////////////////////////

TInstanceLimitsTracker::TInstanceLimitsTracker(
    IInstancePtr instance,
    IInvokerPtr invoker,
    TDuration updatePeriod)
    : Instance_(std::move(instance))
    , Invoker_(std::move(invoker))
    , Executor_(New<NConcurrency::TPeriodicExecutor>(
        Invoker_,
        BIND(&TInstanceLimitsTracker::DoUpdateLimits, MakeWeak(this)),
        updatePeriod))
{ }

void TInstanceLimitsTracker::Start()
{
    Executor_->Start();
    YT_LOG_INFO("Instance limits tracker started");
}

void TInstanceLimitsTracker::DoUpdateLimits()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Checking for instance limits update");

    try {
        auto limits = Instance_->GetResourceLimitsRecursive();
        bool limitsUpdated = false;

        if (CpuLimit_ != limits.Cpu) {
            YT_LOG_INFO("Instance CPU limit updated (OldCpuLimit: %v, NewCpuLimit: %v)",
                CpuLimit_,
                limits.Cpu);
            CpuLimit_ = limits.Cpu;
            limitsUpdated = true;
        }

        if (MemoryLimit_ != limits.Memory) {
            YT_LOG_INFO("Instance memory limit updated (OldMemoryLimit: %v, NewMemoryLimit: %v)",
                MemoryLimit_,
                limits.Memory);
            MemoryLimit_ = limits.Memory;
            limitsUpdated = true;
        }

        if (limitsUpdated) {
            LimitsUpdated_.Fire(*CpuLimit_, *MemoryLimit_);
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get instance limits");
    }
}

////////////////////////////////////////////////////////////////////////////////

TInstanceLimitsTrackerPtr CreateSelfPortoInstanceLimitsTracker(
    IPortoExecutorPtr executor,
    IInvokerPtr invoker,
    TDuration updatePeriod)
{
#ifdef _linux_
    return New<TInstanceLimitsTracker>(GetSelfPortoInstance(executor), invoker, updatePeriod);
#else
    return nullptr;
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainters
