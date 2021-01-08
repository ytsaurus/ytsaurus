#pragma once

#include "helpers.h"
#include "resource_vector.h"

#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/spinlock.h>

#include <yt/core/misc/atomic_object.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceTreeElement
    : public TRefCounted
{
public:
    TResourceTreeElement(TResourceTree* resourceTree, const TString& id, EResourceTreeElementKind elementKind);

    TJobResources GetResourceUsage();

    TJobResources GetResourceUsageWithPrecommit();

    inline TJobMetrics GetJobMetrics();

    bool CheckDemand(
        const TJobResources& delta,
        const TJobResources& resourceDemand,
        const TJobResources& resourceDiscount);

    void SetResourceLimits(TJobResources resourceLimits);

    inline bool GetAlive() const;
    inline void SetNonAlive();

    inline TResourceVector GetFairShare() const;
    inline void SetFairShare(TResourceVector fairShare);

    inline const TString& GetId();

    void MarkInitialized();

private:
    TResourceTree* ResourceTree_;
    const TString Id_;
    const EResourceTreeElementKind Kind_;

    NConcurrency::TPaddedReaderWriterSpinLock ResourceUsageLock_;
    TJobResources ResourceLimits_ = TJobResources::Infinite();
    TJobResources ResourceUsage_;
    TJobResources ResourceUsagePrecommit_;

    std::atomic<bool> ResourceLimitsSpecified_ = false;

    NConcurrency::TPaddedReaderWriterSpinLock JobMetricsLock_;
    TJobMetrics JobMetrics_;

    // NB: all usages of this field must be in TResourceTree.
    TResourceTreeElementPtr Parent_;

    // Element is considered as initialized after first AttachParent.
    bool Initialized_ = false;

    // NB: Any resource usage changes are forbidden after alive is set to false.
    std::atomic<bool> Alive_ = {true};
    TAtomicObject<TResourceVector> FairShare_ = {};

    bool IncreaseLocalResourceUsagePrecommitWithCheck(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput);

    bool IncreaseLocalResourceUsagePrecommit(const TJobResources& delta);

    bool CommitLocalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    bool IncreaseLocalResourceUsage(const TJobResources& delta);

    void ReleaseResources(TJobResources* usagePrecommit, TJobResources* usage);

    inline void ApplyLocalJobMetricsDelta(const TJobMetrics& delta);

    TJobResources GetResourceUsagePrecommit();

    friend class TResourceTree;
};

DEFINE_REFCOUNTED_TYPE(TResourceTreeElement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define RESOURCE_TREE_ELEMENT_INL_H_
#include "resource_tree_element-inl.h"
#undef RESOURCE_TREE_ELEMENT_INL_H_
