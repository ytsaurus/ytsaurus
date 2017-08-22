#ifndef AUTO_MERGE_TASK_INL_H
#error "Direct inclusion of this file is not allowed, include auto_merge_task.h"
#endif

#include "job_info.h"

namespace NYT {
namespace NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingTask>
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

        for (const auto& edgeDescriptor : this->EdgeDescriptors_) {
            edgeDescriptor.DestinationPool->Finish();
        }
    }

    virtual void OnJobStarted(TJobletPtr joblet) override
    {
        TUnderlyingTask::OnJobStarted(joblet);

        // This job is going to be scheduled, we do not have any better estimate any more.
        LastChunkCount_ = 1;

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobStarted(joblet->InputStripeList->TotalChunkCount);
    }

    virtual bool ValidateChunkCount(int chunkCount) override
    {
        CanScheduleJob_ = this->TaskHost_->GetAutoMergeDirector()->CanScheduleTaskJob(chunkCount);
        LastChunkCount_ = chunkCount;
        return CanScheduleJob_;
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
        Persist(context, LastChunkCount_);
    }

private:
    bool CanScheduleJob_ = true;
    // Our current best estimate to the number of chunks in the next job we are able to schedule.
    int LastChunkCount_ = 1;

    void UpdateSelf()
    {
        if (this->IsCompleted()) {
            return;
        }
        CanScheduleJob_ = this->TaskHost_->GetAutoMergeDirector()->CanScheduleTaskJob(LastChunkCount_ /* intermediateChunkCount */);
        if (CanScheduleJob_) {
            this->TaskHost_->AddTaskPendingHint(this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NControllerAgent
} // namespace NYT