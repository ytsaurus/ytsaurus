#include "public.h"
#include "instance_limits_tracker.h"
#include "instance.h"
#include "private.h"

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/ypath_service.h>

namespace NYT::NContainers {

using namespace NYTree;

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
    if (!Running_) {
        Executor_->Start();
        Running_ = true;
        YT_LOG_INFO("Instance limits tracker started");
    }
}

void TInstanceLimitsTracker::Stop()
{
    if (Running_) {
        YT_UNUSED_FUTURE(Executor_->Stop());
        Running_ = false;
        YT_LOG_INFO("Instance limits tracker stopped");
    }
}

void TInstanceLimitsTracker::DoUpdateLimits()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Checking for instance limits update");

    try {
        auto resourceUsage = Instance_->GetResourceUsage({ EStatField::Rss });
        auto it = resourceUsage.find(EStatField::Rss);
        if (it != resourceUsage.end()) {
            const auto& errorOrValue = it->second;
            if (errorOrValue.IsOK()) {
                MemoryUsage_ = errorOrValue.Value();
            } else {
                YT_LOG_ALERT(errorOrValue, "Failed to get container rss");
            }
        } else {
            YT_LOG_ALERT("Failed to get container rss, property not found");
        }

        auto limits = Instance_->GetResourceLimits();
        bool limitsUpdated = false;

        if (CpuGuarantee_ != limits.CpuGuarantee) {
            YT_LOG_INFO("Instance CPU guarantee updated (OldCpuGuarantee: %v, NewCpuGuarantee: %v)",
                CpuGuarantee_,
                limits.CpuGuarantee);
            CpuGuarantee_ = limits.CpuGuarantee;
            // NB: We do not set limitsUpdated since this value used only for diagnostics.
        }

        if (CpuLimit_ != limits.CpuLimit) {
            YT_LOG_INFO("Instance CPU limit updated (OldCpuLimit: %v, NewCpuLimit: %v)",
                CpuLimit_,
                limits.CpuLimit);
            CpuLimit_ = limits.CpuLimit;
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
            LimitsUpdated_.Fire(*CpuLimit_, *CpuGuarantee_, *MemoryLimit_);
        }
    } catch (const std::exception& ex) {
        YT_LOG_WARNING(ex, "Failed to get instance limits");
    }
}

IYPathServicePtr TInstanceLimitsTracker::GetOrchidService()
{
    return IYPathService::FromProducer(BIND(&TInstanceLimitsTracker::DoBuildOrchid, MakeStrong(this)))
        ->Via(Invoker_);
}

void TInstanceLimitsTracker::DoBuildOrchid(NYson::IYsonConsumer* consumer) const
{
    NYTree::BuildYsonFluently(consumer)
        .BeginMap()
            .DoIf(static_cast<bool>(CpuLimit_), [&] (auto fluent) {
                fluent.Item("cpu_limit").Value(*CpuLimit_);
            })
            .DoIf(static_cast<bool>(CpuGuarantee_), [&] (auto fluent) {
                fluent.Item("cpu_guarantee").Value(*CpuGuarantee_);
            })
            .DoIf(static_cast<bool>(MemoryLimit_), [&] (auto fluent) {
                fluent.Item("memory_limit").Value(*MemoryLimit_);
            })
            .DoIf(static_cast<bool>(MemoryUsage_), [&] (auto fluent) {
                fluent.Item("memory_usage").Value(*MemoryUsage_);
            })
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NContainters
