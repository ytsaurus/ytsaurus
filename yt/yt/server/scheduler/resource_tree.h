#pragma once

#include "public.h"
#include "fair_share_tree_element.h"

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/core/concurrency/spinlock.h>

#include <yt/yt/core/misc/lock_free.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

// This class protects structure of shared resource tree, but it doesn't own its elements.
// Each element corresponds to and owned by some scheduler element and all its snapshots.
// There are three general scenarios:
// 1. Get local resource usage of particular element - we take read lock on ResourceUsageLock_ of that element.
// 2. Apply update of some property from leaf to root (increase or decrease of resource usage for example)
// - we take read lock on TreeLock_ for whole operation and make local updates of particular states under write lock on corresponding ResourceUsageLock_.
// 3. Modify tree structure (attach, change or detach parent of element)
// - we take write lock on TreeLock_ for whole operation.
// Only this class is allowed to access Parent_ field of TResourceTreeElement

class TResourceTree
    : public TRefCounted
{
public:
    TResourceTree(
        const TFairShareStrategyTreeConfigPtr& config,
        const std::vector<IInvokerPtr>& feasibleInvokers);

    void UpdateConfig(const TFairShareStrategyTreeConfigPtr& config);

    void IncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta);
    void IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta);
    EResourceTreeIncreaseResult TryIncreaseHierarchicalResourceUsagePrecommit(
        const TResourceTreeElementPtr& element,
        const TJobResources &delta,
        TJobResources *availableResourceLimitsOutput);
    void CommitHierarchicalResourceUsage(
        const TResourceTreeElementPtr& element,
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    void AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent);
    void ChangeParent(
        const TResourceTreeElementPtr& element,
        const TResourceTreeElementPtr& newParent,
        // NB: this argument is required for correct transfer of resource usage.
        const std::optional<std::vector<TResourceTreeElementPtr>>& operationElements);
    void ScheduleDetachParent(const TResourceTreeElementPtr& element);
    void ReleaseResources(const TResourceTreeElementPtr& element, bool markAsNonAlive);

    void ApplyHierarchicalJobMetricsDelta(const TResourceTreeElementPtr& element, const TJobMetrics& delta);

    void PerformPostponedActions();

    void IncrementStructureLockReadCount();
    void IncrementStructureLockWriteCount();
    void IncrementUsageLockReadCount();
    void IncrementUsageLockWriteCount();

    void InitializeResourceUsageFor(
        const TResourceTreeElementPtr& targetElement,
        const std::vector<TResourceTreeElementPtr>& operationElements);

private:
    std::vector<IInvokerPtr> FeasibleInvokers_;

    std::atomic<bool> MaintainInstantResourceUsage_ = false;
    std::atomic<bool> EnableStructureLockProfiling = false;
    std::atomic<bool> EnableUsageLockProfiling = false;

    THashSet<TResourceTreeElementPtr> AliveElements_;

    TMultipleProducerSingleConsumerLockFreeStack<TResourceTreeElementPtr> ElementsToDetachQueue_;
    YT_DECLARE_SPINLOCK(NConcurrency::TReaderWriterSpinLock, StructureLock_);

    NProfiling::TProfiler Profiler_ = NProfiling::TProfiler{"/resource_tree"}.WithHot();
    NProfiling::TCounter StructureLockReadCount_ = Profiler_.Counter("/structure_lock_read_count");
    NProfiling::TCounter StructureLockWriteCount_ = Profiler_.Counter("/structure_lock_write_count");
    NProfiling::TCounter UsageLockReadCount_ = Profiler_.Counter("/usage_read_count");
    NProfiling::TCounter UsageLockWriteCount_ = Profiler_.Counter("/usage_write_count");

    void CheckCycleAbsence(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent);
    void DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta);
    void DoIncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta);

    void DoRecalculateAllResourceUsages();
    
    void DoInitializeResourceUsageFor(
        const TResourceTreeElementPtr& targetElement,
        const std::vector<TResourceTreeElementPtr>& operationElements);
};

DEFINE_REFCOUNTED_TYPE(TResourceTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
