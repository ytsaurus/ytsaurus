#ifndef AUTO_MERGE_TASK_INL_H
#error "Direct inclusion of this file is not allowed, include auto_merge_task.h"
#endif

#include "job_info.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template<class TUnderlyingTask, ui32 tag>
class TAutoMergeabilityMixin
    : public TUnderlyingTask
{
public:
    using TUnderlyingTask::TUnderlyingTask;

    virtual int GetPendingJobCount() const override
    {
        return CanScheduleJob_ ? TUnderlyingTask::GetPendingJobCount() : 0;
    }

    virtual bool CanScheduleJob(NScheduler::ISchedulingContext* context, const TJobResources& jobLimits) override
    {
        return CanScheduleJob_;
    }

    virtual void OnTaskCompleted() override
    {
        TUnderlyingTask::OnTaskCompleted();

        this->TaskHost_->GetAutoMergeDirector()->OnTaskCompleted();

        this->EdgeDescriptors_[0].DestinationPool->Finish();
    }

    virtual void OnJobStarted(TJobletPtr joblet) override
    {
        TUnderlyingTask::OnJobStarted(joblet);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobStarted(joblet->InputStripeList->TotalChunkCount);
    }

    virtual void OnJobAborted(TJobletPtr joblet, const NScheduler::TAbortedJobSummary& jobSummary) override
    {
        TUnderlyingTask::OnJobAborted(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);
    }

    virtual void OnJobFailed(TJobletPtr joblet, const NScheduler::TFailedJobSummary& jobSummary) override
    {
        TUnderlyingTask::OnJobFailed(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);
    }

    virtual void OnJobCompleted(TJobletPtr joblet, NScheduler::TCompletedJobSummary& jobSummary) override
    {
        TUnderlyingTask::OnJobCompleted(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);
    }

    virtual void OnJobLost(TJobletPtr joblet, TCompletedJobPtr completedJob) override
    {
        TUnderlyingTask::OnJobLost(joblet, completedJob);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);
    }

    virtual void SetupCallbacks() override
    {
        TUnderlyingTask::SetupCallbacks();

        this->TaskHost_->GetAutoMergeDirector()->SubscribeStateChanged(BIND(&TAutoMergeabilityMixin::UpdateSelf, MakeWeak(this)));
    }

    virtual TString GetId() const override
    {
        return TUnderlyingTask::GetId() + " + AutoMergeabilityMixin";
    }

    void Persist(const TPersistenceContext& context)
    {
        TUnderlyingTask::Persist(context);

        using NYT::Persist;

        Persist(context, CanScheduleJob_);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAutoMergeabilityMixin, tag);

    bool CanScheduleJob_ = true;

    void UpdateSelf()
    {
        // We do not know how much intermediate chunks our jobs will produce, so we use an optimistic estimate of 1.
        if (this->IsCompleted()) {
            return;
        }
        CanScheduleJob_ = this->TaskHost_->GetAutoMergeDirector()->TryScheduleTaskJob(1 /* intermediateChunkCount */);
        if (CanScheduleJob_) {
            this->TaskHost_->AddTaskPendingHint(this);
        }
    }
};

template <class TUnderlyingTask, ui32 tag>
NPhoenix::TDynamicInitializer<TAutoMergeabilityMixin<TUnderlyingTask, tag>, tag>
    TAutoMergeabilityMixin<TUnderlyingTask, tag>::DynamicPhoenixInitializer;

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT