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

    i64 GetPendingCandidatesDataWeight() const override;

    int GetPendingJobCount() const override;

    int GetTotalJobCount() const override;

    std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> PeekJobCandidate() const;

    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason) override;
    bool OnJobFailed(const TJobletPtr& joblet) override;
    void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie) override;

    void OnJobScheduled(const TJobletPtr& joblet) override;
    bool OnJobCompleted(const TJobletPtr& joblet) override;

    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override;

    bool IsFinished() const override;

    TProgressCounterPtr GetProgressCounter() const override;

protected:
    struct TSecondary
    {
        TJobId JobId;
        TProgressCounterGuard ProgressCounterGuard;

        PHOENIX_DECLARE_TYPE(TSecondary, 0x9f237b98);
    };

    struct TReplicas
        : public TRefCounted
    {
        TJobId MainJobId;
        std::vector<TSecondary> Secondaries;
        int Pending = 0;
        int NotCompleted = 0;

        PHOENIX_DECLARE_TYPE(TReplicas, 0x9f237b97);
    };
    using TReplicasPtr = TIntrusivePtr<TReplicas>;

private:
    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TReplicasPtr> CookieToReplicas_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> PendingCookies_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> BannedCookies_;
    TProgressCounterPtr JobCounter_;

    TTask* Task_;

    NLogging::TSerializableLogger Logger;
    i64 PendingDataWeight_ = 0;

    bool IsRelevant(const TJobletPtr& joblet) const;

    bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        EAbortReason abortReason);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TMultiJobManager, 0xccfb1994);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

