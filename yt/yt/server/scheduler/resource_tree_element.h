#pragma once

#include "helpers.h"
#include "resource_vector.h"

#include <yt/server/lib/scheduler/job_metrics.h>

#include <yt/ytlib/scheduler/job_resources.h>

#include <yt/core/concurrency/rw_spinlock.h>

#include <yt/core/misc/atomic_object.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TResourceTreeElement
    : public TIntrinsicRefCounted
{
public:
    TJobResources GetResourceUsage()
    {
        NConcurrency::TReaderGuard guard(ResourceUsageLock_);

        return ResourceUsage_;
    }

    TJobResources GetResourceUsageWithPrecommit()
    {
        NConcurrency::TReaderGuard guard(ResourceUsageLock_);

        return ResourceUsage_ + ResourceUsagePrecommit_;
    }

    TJobMetrics GetJobMetrics()
    {
        NConcurrency::TReaderGuard guard(JobMetricsLock_);

        return JobMetrics_;
    }

    bool CheckDemand(
        const TJobResources& delta,
        const TJobResources& resourceDemand,
        const TJobResources& resourceDiscount)
    {
        NConcurrency::TReaderGuard guard(ResourceUsageLock_);

        auto availableDemand = ComputeAvailableResources(
            resourceDemand,
            ResourceUsage_ + ResourceUsagePrecommit_,
            resourceDiscount);

        return Dominates(availableDemand, delta);
    }


    inline void SetResourceLimits(TJobResources resourceLimits)
    {
        NConcurrency::TWriterGuard guard(ResourceUsageLock_);

        ResourceLimits_ = resourceLimits;
    }

    inline bool GetAlive() const
    {
        return Alive_.load(std::memory_order_relaxed);
    }

    inline void SetAlive(bool alive)
    {
        Alive_ = alive;
    }

    inline TResourceVector GetFairShare() const
    {
        return FairShare_.Load();
    }

    inline void SetFairShare(TResourceVector fairShare)
    {
        FairShare_.Store(fairShare);
    }

    // NB(antonkikh): For compatibility with the classic scheduler.
    inline double GetFairShareRatio() const
    {
        return MaxComponent(FairShare_.Load());
    }

    // NB(antonkikh): For compatibility with the classic scheduler.
    inline void SetFairShareRatio(double fairShare)
    {
        FairShare_.Store(TResourceVector::FromDouble(fairShare));
    }

private:
    NConcurrency::TPaddedReaderWriterSpinLock ResourceUsageLock_;
    TJobResources ResourceUsage_;
    TJobResources ResourceLimits_ = TJobResources::Infinite();
    TJobResources ResourceUsagePrecommit_;

    NConcurrency::TPaddedReaderWriterSpinLock JobMetricsLock_;
    TJobMetrics JobMetrics_;

    // NB: all usages of this field must be in TResourceTree.
    TResourceTreeElementPtr Parent_;

    std::atomic<bool> Alive_ = {true};
    TAtomicObject<TResourceVector> FairShare_ = {};

    bool IncreaseLocalResourceUsagePrecommitWithCheck(
        const TJobResources& delta,
        TJobResources* availableResourceLimitsOutput)
    {
        NConcurrency::TWriterGuard guard(ResourceUsageLock_);

        // NB: Actually tree elements has resource usage discounts (used for scheduling with preemption)
        // that should be considered in this check. But concurrent nature of this shared tree makes hard to consider
        // these discounts here. The only consequence of discounts ignorance is possibly redundant jobs that would
        // be aborted just after being scheduled.
        auto availableResourceLimits = ComputeAvailableResources(
            ResourceLimits_,
            ResourceUsage_ + ResourceUsagePrecommit_,
            {});

        if (!Dominates(availableResourceLimits, delta)) {
            return false;
        }

        ResourceUsagePrecommit_ += delta;

        *availableResourceLimitsOutput = availableResourceLimits;
        return true;
    }

    void IncreaseLocalResourceUsagePrecommit(const TJobResources& delta)
    {
        NConcurrency::TWriterGuard guard(ResourceUsageLock_);

        ResourceUsagePrecommit_ += delta;
    }


    void CommitLocalResourceUsage(
        const TJobResources& resourceUsageDelta,
        const TJobResources& precommittedResources)
    {
        NConcurrency::TWriterGuard guard(ResourceUsageLock_);

        ResourceUsage_ += resourceUsageDelta;
        ResourceUsagePrecommit_ -= precommittedResources;
    }

    void IncreaseLocalResourceUsage(const TJobResources& delta)
    {
        NConcurrency::TWriterGuard guard(ResourceUsageLock_);

        ResourceUsage_ += delta;
    }

    void ApplyLocalJobMetricsDelta(const TJobMetrics& delta)
    {
        NConcurrency::TWriterGuard guard(JobMetricsLock_);

        JobMetrics_ += delta;
    }

    TJobResources GetResourceUsagePrecommit()
    {
        NConcurrency::TReaderGuard guard(ResourceUsageLock_);

        return ResourceUsagePrecommit_;
    }

    friend class TResourceTree;
};

DEFINE_REFCOUNTED_TYPE(TResourceTreeElement)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
