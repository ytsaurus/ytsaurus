#pragma once

#include "helpers.h"

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/core/misc/atomic_object.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceTreeElement
    : public TRefCounted
{
public:
    TResourceTreeElement(
        TResourceTree* resourceTree,
        const TString& id,
        EResourceTreeElementKind elementKind);

    TJobResources GetResourceUsage();

    TJobResources GetResourceUsageWithPrecommit();

    bool CheckAvailableDemand(
        const TJobResources& delta,
        const TJobResources& resourceDemand);

    void SetResourceLimits(
        const TJobResources& resourceLimits,
        const std::vector<TResourceTreeElementPtr>& descendantOperations);
    bool AreResourceLimitsViolated() const;

    bool ResourceLimitsSpecified() const;

    inline bool GetAlive() const;
    inline void SetNonAlive();

    inline const TString& GetId();

    void MarkInitialized();

private:
    TResourceTree* ResourceTree_;
    const TString Id_;
    const EResourceTreeElementKind Kind_;

    NThreading::TPaddedReaderWriterSpinLock ResourceUsageLock_;
    TJobResources ResourceLimits_ = TJobResources::Infinite();
    TJobResources ResourceUsage_;
    TJobResources ResourceUsagePrecommit_;

    std::atomic<bool> ResourceLimitsSpecified_ = false;

    // NB: all usages of this field must be in TResourceTree.
    TResourceTreeElementPtr Parent_;

    // Element is considered as initialized after first AttachParent.
    bool Initialized_ = false;

    // NB: Any resource usage changes are forbidden after alive is set to false.
    std::atomic<bool> Alive_ = {true};

    bool IncreaseLocalResourceUsagePrecommitWithCheck(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput);

    bool IncreaseLocalResourceUsagePrecommit(const TJobResources& delta);

    bool CommitLocalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    bool IncreaseLocalResourceUsage(const TJobResources& delta);

    void ReleaseResources(TJobResources* usagePrecommit, TJobResources* usage);

    TJobResources GetResourceUsagePrecommit();

    friend class TResourceTree;
};

DEFINE_REFCOUNTED_TYPE(TResourceTreeElement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler

#define RESOURCE_TREE_ELEMENT_INL_H_
#include "resource_tree_element-inl.h"
#undef RESOURCE_TREE_ELEMENT_INL_H_
