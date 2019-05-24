#pragma once

#include "public.h"
#include "fair_share_tree_element.h"

#include <yt/server/lib/scheduler/job_metrics.h>

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
// Only this class is allowed to access Parent_ field of TSchedulerElementSharedState

class TResourceTree
    : public TRefCounted
{
public:
    void IncreaseHierarchicalResourceUsage(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta);
    void IncreaseHierarchicalResourceUsagePrecommit(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta);
    bool TryIncreaseHierarchicalResourceUsagePrecommit(
        const TSchedulerElementSharedStatePtr& element,
        const TJobResources &delta,
        TJobResources *availableResourceLimitsOutput);
    void CommitHierarchicalResourceUsage(
        const TSchedulerElementSharedStatePtr& element,
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    void AttachParent(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& parent);
    void ChangeParent(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& newParent);
    void DetachParent(const TSchedulerElementSharedStatePtr& element);
    void ReleaseResources(const TSchedulerElementSharedStatePtr& element);

    void ApplyHierarchicalJobMetricsDelta(const TSchedulerElementSharedStatePtr& element, const TJobMetrics& delta);

private:
    NConcurrency::TReaderWriterSpinLock TreeLock_;

    void CheckCycleAbsence(const TSchedulerElementSharedStatePtr& element, const TSchedulerElementSharedStatePtr& newParent);
    void DoIncreaseHierarchicalResourceUsage(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta);
    void DoIncreaseHierarchicalResourceUsagePrecommit(const TSchedulerElementSharedStatePtr& element, const TJobResources& delta);
};


DEFINE_REFCOUNTED_TYPE(TResourceTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
