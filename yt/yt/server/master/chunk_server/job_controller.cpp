#include "job_controller.h"

#include "job.h"

namespace NYT::NChunkServer {

////////////////////////////////////////////////////////////////////////////////

class TCompositeJobController
    : public ICompositeJobController
{
public:
    // IJobController implementation.
    void ScheduleJobs(IJobSchedulingContext* context) override
    {
        for (const auto& jobController : JobControllers_) {
            jobController->ScheduleJobs(context);
        }
    }

    void OnJobWaiting(const TJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        const auto& jobController = GetControllerForJob(job);
        jobController->OnJobWaiting(job, callbacks);
    }

    void OnJobRunning(const TJobPtr& job, IJobControllerCallbacks* callbacks) override
    {
        const auto& jobController = GetControllerForJob(job);
        jobController->OnJobRunning(job, callbacks);
    }

    void OnJobCompleted(const TJobPtr& job) override
    {
        const auto& jobController = GetControllerForJob(job);
        jobController->OnJobCompleted(job);
    }

    void OnJobAborted(const TJobPtr& job) override
    {
        const auto& jobController = GetControllerForJob(job);
        jobController->OnJobAborted(job);
    }

    void OnJobFailed(const TJobPtr& job) override
    {
        const auto& jobController = GetControllerForJob(job);
        jobController->OnJobFailed(job);
    }

    // ICompositeJobController implementation.
    void RegisterJobController(EJobType jobType, IJobControllerPtr controller) override
    {
        YT_VERIFY(JobTypeToJobController_.emplace(jobType, controller).second);
        JobControllers_.insert(std::move(controller));
    }

private:
    THashMap<EJobType, IJobControllerPtr> JobTypeToJobController_;
    THashSet<IJobControllerPtr> JobControllers_;

    const IJobControllerPtr& GetControllerForJob(const TJobPtr& job) const
    {
        return GetOrCrash(JobTypeToJobController_, job->GetType());
    }
};

////////////////////////////////////////////////////////////////////////////////

ICompositeJobControllerPtr CreateCompositeJobController()
{
    return New<TCompositeJobController>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkServer
