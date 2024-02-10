#ifndef AUTO_MERGE_TASK_INL_H
#error "Direct inclusion of this file is not allowed, include auto_merge_task.h"
// For the sake of sane code completion.
#include "auto_merge_task.h"
#endif

#include "job_info.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

template <class TUnderlyingTask>
class TAutoMergeableOutputMixin
    : public TUnderlyingTask
{
public:
    using TUnderlyingTask::TUnderlyingTask;

    virtual TCompositePendingJobCount GetPendingJobCount() const override
    {
        if (!CanScheduleJob_) {
            return TCompositePendingJobCount{};
        }

        auto result = TUnderlyingTask::GetPendingJobCount();
        // NB: automerge works only in regular computation.
        result.DefaultCount = std::min(
            result.DefaultCount,
            this->TaskHost_->GetAutoMergeDirector()->GetTaskPendingJobCountLimit());
        return result;
    }

    virtual std::optional<EScheduleAllocationFailReason> GetScheduleFailReason(ISchedulingContext* /*context*/) override
    {
        return CanScheduleJob_ ? std::nullopt : std::make_optional(EScheduleAllocationFailReason::TaskRefusal);
    }

    virtual void OnTaskCompleted() override
    {
        TUnderlyingTask::OnTaskCompleted();

        this->TaskHost_->GetAutoMergeDirector()->OnTaskCompleted();

        for (const auto& streamDescriptor : this->OutputStreamDescriptors_) {
            streamDescriptor->DestinationPool->Finish();
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

    virtual TJobFinishedResult OnJobAborted(TJobletPtr joblet, const TAbortedJobSummary& jobSummary) override
    {
        auto result = TUnderlyingTask::OnJobAborted(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);

        return result;
    }

    virtual TJobFinishedResult OnJobFailed(TJobletPtr joblet, const TFailedJobSummary& jobSummary) override
    {
        auto result = TUnderlyingTask::OnJobFailed(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);

        return result;
    }

    virtual TJobFinishedResult OnJobCompleted(TJobletPtr joblet, TCompletedJobSummary& jobSummary) override
    {
        auto result = TUnderlyingTask::OnJobCompleted(joblet, jobSummary);

        this->TaskHost_->GetAutoMergeDirector()->OnTaskJobFinished(joblet->InputStripeList->TotalChunkCount);

        return result;
    }

    virtual void SetupCallbacks() override
    {
        TUnderlyingTask::SetupCallbacks();

        this->TaskHost_->GetAutoMergeDirector()->SubscribeStateChanged(BIND(&TAutoMergeableOutputMixin::UpdateSelf, MakeWeak(this)));
    }

    virtual TString GetTitle() const override
    {
        return TUnderlyingTask::GetTitle() + " + AutoMergeableOutputMixin";
    }

    virtual bool CanLoseJobs() const override
    {
        // If user code is deterministic, it is safe to restart it arbitrarily.
        return this->GetUserJobSpec()->Deterministic;
    }

    virtual void Persist(const TPersistenceContext& context) override
    {
        TUnderlyingTask::Persist(context);

        using NYT::Persist;

        Persist(context, LastChunkCount_);
    }

private:
    // NB: this field is intentionally transient (otherwise automerge can stuck after loading from snapshot).
    bool CanScheduleJob_ = true;
    // Our current best estimate to the number of chunks in the next job we are able to schedule.
    int LastChunkCount_ = 1;

    void UpdateSelf()
    {
        if (this->IsCompleted()) {
            return;
        }
        CanScheduleJob_ = this->TaskHost_->GetAutoMergeDirector()->CanScheduleTaskJob(LastChunkCount_ /*intermediateChunkCount*/);
        if (CanScheduleJob_) {
            this->TaskHost_->UpdateTask(this);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
