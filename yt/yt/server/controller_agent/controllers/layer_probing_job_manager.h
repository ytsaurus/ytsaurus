#pragma once

#include "competitive_job_manager.h"

#include <yt/yt/server/lib/controller_agent/persistence.h>
#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/scheduler/structs.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ELayerProbingJobStatus,
    (LayerProbingJobCompleted)
    (NoLayerProbingJobResult)
)

////////////////////////////////////////////////////////////////////////////////

class TLayerProbingJobManager
    : public TCompetitiveJobManagerBase
{
public:
    TLayerProbingJobManager();

    TLayerProbingJobManager(
        ICompetitiveJobManagerHost* host,
        NLogging::TLogger logger);

    void SetUserJobSpec(NScheduler::TUserJobSpecPtr userJobSpec);

    void OnJobScheduled(const TJobletPtr& joblet) override;
    void OnJobCompleted(const TJobletPtr& joblet) override;

    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override;

    bool IsLayerProbingEnabled() const;
    bool IsLayerProbeReady() const;

    bool ShouldUseProbingLayer() const;
    int FailedJobCount() const;

    NJobTrackerClient::TJobId GetFailedLayerProbingJob() const;
    NJobTrackerClient::TJobId GetFailedNonLayerProbingJob() const;

    void Persist(const TPersistenceContext& context);

private:
    NJobTrackerClient::TJobId FailedLayerProbingJob_;
    NJobTrackerClient::TJobId FailedNonLayerProbingJob_;
    NScheduler::TUserJobSpecPtr UserJobSpec_;
    int UnsuccessfulJobCount_ = 0;
    int LayerProbingRunLost_ = 0;
    ELayerProbingJobStatus LayerProbingStatus_ = ELayerProbingJobStatus::NoLayerProbingJobResult;

    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter) override;

    bool IsLayerProbeRequired() const;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
