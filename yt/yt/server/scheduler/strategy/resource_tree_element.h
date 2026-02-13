#pragma once

#include "private.h"

#include <yt/yt/server/lib/scheduler/job_metrics.h>

#include <yt/yt/library/vector_hdrf/job_resources.h>
#include <yt/yt/library/vector_hdrf/resource_vector.h>

#include <library/cpp/yt/threading/atomic_object.h>
#include <library/cpp/yt/threading/rw_spin_lock.h>

namespace NYT::NScheduler::NStrategy {

////////////////////////////////////////////////////////////////////////////////

class TResourceTreeElement
    : public TRefCounted
{
public:
    struct TDetailedResourceUsage
    {
        TJobResources Base;
        TJobResources Precommit;
    };

    TResourceTreeElement(
        TResourceTree* resourceTree,
        const TString& id,
        EResourceTreeElementKind elementKind);

    TJobResources GetResourceUsage();
    TJobResources GetResourceUsageWithPrecommit();
    TDetailedResourceUsage GetDetailedResourceUsage();

    bool CheckAvailableDemand(
        const TJobResources& delta,
        const TJobResources& resourceDemand);

    void SetSpecifiedResourceLimits(
        const std::optional<TJobResources>& specifiedResourceLimits,
        const TJobResources& overcommitTolerance,
        const std::vector<TResourceTreeElementPtr>& descendantOperations);
    bool AreSpecifiedResourceLimitsViolated(bool considerPreemptedPrecommited = false) const;

    bool AreResourceLimitsSpecified() const;

    inline bool GetAlive() const;
    inline void SetNonAlive();

    NThreading::TWriterGuard<NThreading::TPaddedReaderWriterSpinLock> AcquireWriteLock();

    inline const TString& GetId();

    void MarkInitialized();

private:
    TResourceTree* ResourceTree_;
    const TString Id_;
    const EResourceTreeElementKind Kind_;

    NThreading::TPaddedReaderWriterSpinLock ResourceUsageLock_;
    std::optional<TJobResources> SpecifiedResourceLimits_;
    TJobResources SpecifiedResourceLimitsOvercommitTolerance_;
    TJobResources ResourceUsage_;
    TJobResources ResourceUsagePrecommit_;
    TJobResources PreemptedResourceUsagePrecommit_;

    std::atomic<bool> ResourceLimitsSpecified_ = false;

    // NB: All usages of this field must be in TResourceTree.
    TResourceTreeElementPtr Parent_;

    // Element is considered as initialized after first AttachParent.
    bool Initialized_ = false;

    // NB: Any resource usage changes are forbidden after alive is set to false.
    std::atomic<bool> Alive_ = true;

    bool AreSpecifiedResourceLimitsViolatedUnsafe(bool considerPreemptedPrecommited = false) const;

    EResourceTreeIncreaseResult IncreaseLocalResourceUsagePrecommitWithCheck(
        const TJobResources& delta,
        bool allowLimitsOvercommit,
        TJobResources* availableResourceLimitsOutput);

    EResourceTreeIncreaseResult IncreaseLocalResourceUsagePrecommitWithCheckUnsafe(
        const TJobResources& delta,
        bool allowLimitsOvercommit,
        const std::optional<TJobResources>& additionalLocalResourceLimits,
        TJobResources* availableResourceLimitsOutput);

    bool IncreaseLocalResourceUsagePrecommit(
        const TJobResources& delta,
        bool enableDetailedLogs = false);

    bool IncreaseLocalResourceUsagePrecommitUnsafe(
        const TJobResources& delta,
        bool enableDetailedLogs = false);

    bool IncreaseLocalPreemptedResourceUsagePrecommit(const TJobResources& delta);
    bool IncreaseLocalPreemptedResourceUsagePrecommitUnsafe(const TJobResources& delta);
    bool ResetLocalPreemptedResourceUsagePrecommit();

    bool CommitLocalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources);

    bool CommitLocalPreemptedResourceUsage(const TJobResources& resourceUsageDelta);

    bool IncreaseLocalResourceUsage(const TJobResources& delta);

    void ReleaseResources(
        TJobResources* usagePrecommit,
        TJobResources* usage,
        TJobResources* preemptedUsagePrecommit);

    TJobResources GetResourceUsagePrecommit();
    TJobResources GetPreemptedResourceUsagePrecommit();

    friend class TResourceTree;
};

DEFINE_REFCOUNTED_TYPE(TResourceTreeElement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler::NStrategy

#define RESOURCE_TREE_ELEMENT_INL_H_
#include "resource_tree_element-inl.h"
#undef RESOURCE_TREE_ELEMENT_INL_H_
