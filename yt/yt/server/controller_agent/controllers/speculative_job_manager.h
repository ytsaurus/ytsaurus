#pragma once

#include "competitive_job_manager.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Speculative job are the copy of another slow job that launched on the same data.
//! Some jobs can be very slow due to load of underlying node and if this job is one of the last, the operation time can be significantly increased.
//! Speculative job launched on another node can perform faster so operation will finish earlier.
//! TSpeculativeJobManager tracks these jobs and if one of the job processing the same data is completed, other jobs are aborted.

class TSpeculativeJobManager
    : public TCompetitiveJobManagerBase
{
public:
    //! Used only for persistence.
    TSpeculativeJobManager() = default;

    TSpeculativeJobManager(
        ICompetitiveJobManagerHost* host,
        NLogging::TLogger logger,
        int maxSpeculativeJobCount);

    void OnJobScheduled(const TJobletPtr& joblet) override;
    void OnJobCompleted(const TJobletPtr& joblet) override;

    // If competitive job of this joblet completed we should abort the joblet even if it has completed.
    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override;

private:
    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter) override;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
