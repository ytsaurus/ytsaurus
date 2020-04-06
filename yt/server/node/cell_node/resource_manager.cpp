#include "resource_manager.h"
#include "private.h"
#include "bootstrap.h"
#include "config.h"

#include <yt/server/node/exec_agent/slot_manager.h>

#include <yt/server/node/tablet_node/slot_manager.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <limits>

namespace NYT::NCellNode {

using namespace NConcurrency;
using namespace NNodeTrackerClient;
using namespace NNodeTrackerClient::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CellNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMemoryLimitPtr GetMemoryLimit(const TResourceLimitsConfigPtr& config, EMemoryCategory category)
{
    switch (category) {
    case EMemoryCategory::UserJobs:
        return config->UserJobs;
    case EMemoryCategory::TabletStatic:
        return config->TabletStatic;
    case EMemoryCategory::TabletDynamic:
        return config->TabletDynamic;
    default:
        return nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

TNodeResourceManager::TNodeResourceManager(
    IInvokerPtr invoker,
    TBootstrap* bootstrap,
    TDuration updatePeriod)
    : Invoker_(std::move(invoker))
    , Bootstrap_(bootstrap)
    , Config_(Bootstrap_->GetConfig()->ResourceLimits)
    , UpdateExecutor_(New<TPeriodicExecutor>(
        Invoker_,
        BIND(&TNodeResourceManager::UpdateLimits, MakeWeak(this)),
        updatePeriod))
    , TotalCpu_(Config_->TotalCpu)
    , TotalMemory_(Config_->TotalMemory)
{ }

void TNodeResourceManager::Start()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Starting node resource manager");

    UpdateExecutor_->Start();
}

void TNodeResourceManager::OnInstanceLimitsUpdated(double cpuLimit, i64 memoryLimit)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_INFO("Instance limits updated (OldCpuLimit: %v, NewCpuLimit: %v, OldMemoryLimit: %v, NewMemoryLimit: %v)",
        TotalCpu_,
        cpuLimit,
        TotalMemory_,
        memoryLimit);

    TotalCpu_ = cpuLimit;
    TotalMemory_ = memoryLimit;
}

double TNodeResourceManager::GetJobsCpuLimit() const
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    return JobsCpuLimit_;
}

void TNodeResourceManager::SetResourceLimitsOverride(const TNodeResourceLimitsOverrides& resourceLimitsOverride)
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    ResourceLimitsOverride_ = resourceLimitsOverride;
}

void TNodeResourceManager::UpdateLimits()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    YT_LOG_DEBUG("Updating node resource limits");

    UpdateMemoryFootprint();
    UpdateMemoryLimits();
    UpdateJobsCpuLimit();
}

void TNodeResourceManager::UpdateMemoryLimits()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    int dynamicCategoryCount = 0;
    i64 totalDynamicMemory = TotalMemory_ - *Config_->FreeMemoryWatermark;

    auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    memoryUsageTracker->SetTotalLimit(TotalMemory_);

    TEnumIndexedVector<EMemoryCategory, i64> newLimits;
    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto memoryLimit = GetMemoryLimit(Config_, category);
        if (!memoryLimit || !memoryLimit->Type || memoryLimit->Type == EMemoryLimitType::None) {
            newLimits[category] = std::numeric_limits<i64>::max();
            totalDynamicMemory -= memoryUsageTracker->GetUsed(category);
        } else if (memoryLimit->Type == EMemoryLimitType::Static) {
            newLimits[category] = *memoryLimit->Value;
            totalDynamicMemory -= *memoryLimit->Value;
        } else {
            ++dynamicCategoryCount;
        }
    }

    if (dynamicCategoryCount > 0) {
        auto dynamicMemoryPerCategory = std::max<i64>(totalDynamicMemory / dynamicCategoryCount, 0);
        for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
            auto memoryLimit = GetMemoryLimit(Config_, category);
            if (memoryLimit && memoryLimit->Type == EMemoryLimitType::Dynamic) {
                newLimits[category] = dynamicMemoryPerCategory;
            }
        }
    }

    if (ResourceLimitsOverride_.has_system_memory()) {
        newLimits[EMemoryCategory::SystemJobs] = ResourceLimitsOverride_.system_memory();
    }
    if (ResourceLimitsOverride_.has_user_memory()) {
        newLimits[EMemoryCategory::UserJobs] = ResourceLimitsOverride_.user_memory();
    }

    for (auto category : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        auto oldLimit = memoryUsageTracker->GetLimit(category);
        auto newLimit = newLimits[category];

        if (oldLimit != newLimit) {
            YT_LOG_INFO("Updating memory category limit (Category: %v, OldLimit: %v, NewLimit: %v)",
                category,
                oldLimit,
                newLimit);
            memoryUsageTracker->SetCategoryLimit(category, newLimit);
        }
    }
}

void TNodeResourceManager::UpdateMemoryFootprint()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    const auto& memoryUsageTracker = Bootstrap_->GetMemoryUsageTracker();

    auto bytesCommitted = NYTAlloc::GetTotalAllocationCounters()[NYTAlloc::ETotalCounter::BytesCommitted];
    auto newFootprint = bytesCommitted;
    for (auto memoryCategory : TEnumTraits<EMemoryCategory>::GetDomainValues()) {
        if (memoryCategory == EMemoryCategory::UserJobs || memoryCategory == EMemoryCategory::Footprint) {
            continue;
        }
        newFootprint -= memoryUsageTracker->GetUsed(memoryCategory);
    }
    newFootprint = std::max<i64>(newFootprint, 0);

    auto oldFootprint = Bootstrap_->GetMemoryUsageTracker()->GetUsed(EMemoryCategory::Footprint);

    YT_LOG_INFO("Memory footprint updated (BytesCommitted: %v, OldFootprint: %v, NewFootprint: %v)",
        bytesCommitted,
        oldFootprint,
        newFootprint);

    if (newFootprint > oldFootprint) {
        memoryUsageTracker->Acquire(
            EMemoryCategory::Footprint,
            newFootprint - oldFootprint);
    } else {
        memoryUsageTracker->Release(
            EMemoryCategory::Footprint,
            oldFootprint - newFootprint);
    }
}

void TNodeResourceManager::UpdateJobsCpuLimit()
{
    VERIFY_INVOKER_AFFINITY(Invoker_);

    double newJobsCpuLimit = 0;

    // COPMAT(gritukan)
    if (ResourceLimitsOverride_.has_cpu()) {
        newJobsCpuLimit = ResourceLimitsOverride_.cpu();
    } else {
        if (TotalCpu_) {
            newJobsCpuLimit = *TotalCpu_ - *Config_->NodeDedicatedCpu;
        } else {
            newJobsCpuLimit = Bootstrap_->GetConfig()->ExecAgent->JobController->ResourceLimits->Cpu;
        }

        newJobsCpuLimit -= Bootstrap_->GetTabletSlotManager()->GetUsedCpu(*Config_->CpuPerTabletSlot);
    }
    newJobsCpuLimit = std::max<double>(newJobsCpuLimit, 0);

    auto slotManager = Bootstrap_->GetExecSlotManager();
    if (slotManager) {
        try {
            Bootstrap_->GetExecSlotManager()->UpdateCpuLimit(newJobsCpuLimit);
        } catch (const std::exception& ex) {
            YT_LOG_WARNING(ex, "Failed to update jobs cpu limit");
            return;
        }
    }
    
    JobsCpuLimit_ = newJobsCpuLimit;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode
