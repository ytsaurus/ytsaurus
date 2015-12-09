#pragma once

#include "job.h"

#include <yt/ytlib/object_client/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct ISchedulingContext
{
    virtual ~ISchedulingContext()
    { }


    virtual TExecNodePtr GetNode() const = 0;
    virtual Stroka GetAddress() const = 0;

    virtual const TJobResources& ResourceLimits() const = 0;
    virtual TJobResources& ResourceUsageDiscount() = 0;

    virtual const std::vector<TJobPtr>& StartedJobs() const = 0;
    virtual const std::vector<TJobPtr>& PreemptedJobs() const = 0;
    virtual const std::vector<TJobPtr>& RunningJobs() const = 0;

    virtual TJobPtr FindStartedJob(const TJobId& jobId) const = 0;

    virtual bool CanStartMoreJobs() const = 0;

    virtual TJobId StartJob(
        TOperationPtr operation,
        EJobType type,
        const TJobResources& resourceLimits,
        bool restarted,
        TJobSpecBuilder specBuilder) = 0;

    virtual void PreemptJob(TJobPtr job) = 0;

    virtual TInstant GetNow() const = 0;
};

std::unique_ptr<ISchedulingContext> CreateSchedulingContext(
    TSchedulerConfigPtr config,
    TExecNodePtr node,
    const std::vector<TJobPtr>& runningJobs,
    NObjectClient::TCellTag cellTag);

////////////////////////////////////////////////////////////////////////////////

class TSchedulingContextBase
    : public ISchedulingContext
{
public:
    DEFINE_BYVAL_RO_PROPERTY(TExecNodePtr, Node);

    //! Used during preemption to allow second-chance scheduling.
    DEFINE_BYREF_RW_PROPERTY(TJobResources, ResourceUsageDiscount);

    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, StartedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, PreemptedJobs);
    DEFINE_BYREF_RO_PROPERTY(std::vector<TJobPtr>, RunningJobs);

public:
    TSchedulingContextBase(
        TSchedulerConfigPtr config,
        TExecNodePtr node,
        const std::vector<TJobPtr>& runningJobs,
        NObjectClient::TCellTag cellTag);

    virtual Stroka GetAddress() const override;

    virtual const TJobResources& ResourceLimits() const override;

    virtual TJobPtr FindStartedJob(const TJobId& jobId) const override;

    virtual bool CanStartMoreJobs() const override;

    virtual TJobId StartJob(
        TOperationPtr operation,
        EJobType type,
        const TJobResources& resourceLimits,
        bool restarted,
        TJobSpecBuilder specBuilder) override;

    virtual void PreemptJob(TJobPtr job) override;

    virtual TInstant GetNow() const override;

private:
    const TSchedulerConfigPtr Config_;
    const NObjectClient::TCellTag CellTag_;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
