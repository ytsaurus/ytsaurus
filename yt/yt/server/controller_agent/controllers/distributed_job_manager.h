#pragma once

#include "competitive_job_manager.h"

#include <util/generic/vector.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Multiple jobs are created when CookieGroupSize is >1.
//! The cookie group counts as completed when all the jobs successfully complete.
//! When some jobs fail or abort, then it gets restarted.

class TDistributedJobManager
    : public IExtraJobManager
{
public:
    //! Used only for persistence.
    TDistributedJobManager() = default;

    TDistributedJobManager(
        TTask* host,
        NLogging::TLogger logger);

    i64 GetPendingCandidatesDataWeight() const override final;

    int GetPendingJobCount() const override final;

    int GetTotalJobCount() const override final;

    std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> PeekJobCandidate() const;

    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason) override final;
    bool OnJobFailed(const TJobletPtr& joblet) override final;
    void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie) override final;

    void OnJobScheduled(const TJobletPtr& joblet) override final;
    bool OnJobCompleted(const TJobletPtr& joblet) override final;

    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override final;

    bool IsFinished() const override final;

    TProgressCounterPtr GetProgressCounter() const override final;

    int GetCookieGroupSize() const;

private:
    struct TSecondary
    {
        TJobId JobId;
        TProgressCounterGuard ProgressCounterGuard;

        PHOENIX_DECLARE_TYPE(TSecondary, 0x9f237b98);
    };

    struct TReplicas
    {
        TJobId MainJobId;
        TCompactVector<TSecondary, 3> Secondaries;
        int Pending = 0;
        int NotCompletedCount = 0;

        PHOENIX_DECLARE_TYPE(TReplicas, 0x9f237b97);
    };

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TReplicas> CookieToReplicas_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> PendingCookies_;
    TProgressCounterPtr JobCounter_;

    TTask* Task_;

    NLogging::TSerializableLogger Logger;

    bool IsRelevant() const;

    bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        EAbortReason abortReason);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TDistributedJobManager, 0xccfb1994);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

