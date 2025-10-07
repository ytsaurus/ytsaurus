#pragma once

#include "competitive_job_manager.h"

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/scheduler/structs.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Probing job are the copy of another job that launched on the same data but in another pool tree.
//! They allow us to compare performance of different pool trees (i. e. physical and cloud).
//! TProbingJobManager tracks these jobs and reacts on their completion/failure.
//! More details: https://wiki.yandex-team.ru/users/renadeen/probing-jobs/

class TProbingJobManager
    : public TCompetitiveJobManagerBase
{
public:
    //! Used only for persistence.
    TProbingJobManager();
    TProbingJobManager& operator=(const TProbingJobManager& other);

    TProbingJobManager(
        ICompetitiveJobManagerHost* host,
        NLogging::TLogger logger,
        int maxProbingJobCount,
        std::optional<double> probingRatio,
        std::optional<std::string> probingPoolTreeId);

    void OnJobScheduled(const TJobletPtr& joblet) override;
    bool OnJobCompleted(const TJobletPtr& joblet) override;

    // If competitive job of this joblet completed we should abort the joblet even if it has completed.
    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override;

    void UpdatePendingJobCount(TCompositePendingJobCount* pendingJobCount) const;

private:
    std::optional<double> ProbingRatio_;
    std::optional<std::string> ProbingPoolTreeId_;

    std::random_device RandomDevice_;
    std::mt19937 RandomGenerator_;

    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
        NJobTrackerClient::EJobState state) override;

    PHOENIX_DECLARE_TYPE(TProbingJobManager, 0x5137fadd);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
