#pragma once

#include "competitive_job_manager.h"

#include <util/generic/vector.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Speculative job are the copy of another slow job that launched on the same data.
//! Some jobs can be very slow due to load of underlying node and if this job is one of the last, the operation time can be significantly increased.
//! Speculative job launched on another node can perform faster so operation will finish earlier.
//! TSpeculativeJobManager tracks these jobs and if one of the job processing the same data is completed, other jobs are aborted.

class TMultiJobManager
    : public IExtraJobManager
{
public:
    //! Used only for persistence.
    TMultiJobManager() = default;

    TMultiJobManager(
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

protected:
    struct TSecondary
    {
        TJobId JobId;
        TProgressCounterGuard ProgressCounterGuard;

        PHOENIX_DECLARE_TYPE(TSecondary, 0x9f237b98);
    };

    struct TReplicas
    {
        TJobId MainJobId;
        std::vector<TSecondary> Secondaries;
        int Pending = 0;
        int NotCompletedCount = 0;

        PHOENIX_DECLARE_TYPE(TReplicas, 0x9f237b97);
    };

private:
    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TReplicas> CookieToReplicas_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> PendingCookies_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> BannedCookies_;
    TProgressCounterPtr JobCounter_;

    TTask* Task_;

    NLogging::TSerializableLogger Logger;

    bool IsRelevant(const TJobletPtr& joblet) const;

    bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        EAbortReason abortReason);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TMultiJobManager, 0xccfb1994);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

