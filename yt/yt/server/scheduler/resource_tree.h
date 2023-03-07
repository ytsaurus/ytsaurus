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
// Only this class is allowed to access Parent_ field of TResourceTreeElement

class TResourceTree
    : public TRefCounted
{
public:
    void IncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta);
    void IncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta);
    bool TryIncreaseHierarchicalResourceUsagePrecommit(
        const TResourceTreeElementPtr& element,
        const TJobResources &delta,
        TJobResources *availableResourceLimitsOutput);
    void CommitHierarchicalResourceUsage(
        const TResourceTreeElementPtr& element,
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    void AttachParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& parent);
    void ChangeParent(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent);
    void DetachParent(const TResourceTreeElementPtr& element);
    void ReleaseResources(const TResourceTreeElementPtr& element);

    void ApplyHierarchicalJobMetricsDelta(const TResourceTreeElementPtr& element, const TJobMetrics& delta);

private:
    NConcurrency::TReaderWriterSpinLock TreeLock_;

    void CheckCycleAbsence(const TResourceTreeElementPtr& element, const TResourceTreeElementPtr& newParent);
    void DoIncreaseHierarchicalResourceUsage(const TResourceTreeElementPtr& element, const TJobResources& delta);
    void DoIncreaseHierarchicalResourceUsagePrecommit(const TResourceTreeElementPtr& element, const TJobResources& delta);
};


DEFINE_REFCOUNTED_TYPE(TResourceTree)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
