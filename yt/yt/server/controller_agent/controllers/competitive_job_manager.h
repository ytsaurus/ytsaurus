#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECompetitionStatus,
    (SingleJobOnly)
    (TwoCompetitiveJobs)
    (CompetitionCompleted)
)

class TCompetitiveJobManager
{
public:
    //! Used only for persistence.
    TCompetitiveJobManager() = default;

    TCompetitiveJobManager(
        std::function<void(const TJobletPtr&)> onSpeculativeJobScheduled,
        std::function<void(TJobId, EAbortReason)> abortJobCallback,
        NLogging::TLogger logger,
        int maxSpeculativeJobCount);

    bool TryRegisterSpeculativeCandidate(const TJobletPtr& joblet);

    int GetPendingSpeculativeJobCount() const;

    int GetTotalSpeculativeJobCount() const;

    NChunkPools::IChunkPoolOutput::TCookie PeekSpeculativeCandidate() const;

    void OnJobScheduled(const TJobletPtr& joblet);
    void OnJobCompleted(const TJobletPtr& joblet);

    // Next two methods return whether we must return cookie to chunk pool or not.
    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason);
    bool OnJobFailed(const TJobletPtr& joblet);

    // If competitive job of this joblet completed we should abort the joblet even if it has completed.
    std::optional<EAbortReason> ShouldAbortJob(const TJobletPtr& joblet) const;

    i64 GetPendingCandidatesDataWeight() const;

    bool IsFinished() const;

    TProgressCounterPtr GetProgressCounter() const;

    void Persist(const TPersistenceContext& context);

private:
    struct TCompetition
        : public TRefCounted
    {
        ECompetitionStatus Status = ECompetitionStatus::SingleJobOnly;
        std::vector<TJobId> Competitors;
        TJobId JobCompetitionId;
        i64 PendingDataWeight;
        TProgressCounterGuard ProgressCounterGuard;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Status);
            Persist(context, Competitors);
            Persist(context, JobCompetitionId);
            Persist(context, PendingDataWeight);
            Persist(context, ProgressCounterGuard);
        }
    };
    DEFINE_REFCOUNTED_TYPE(TCompetition)

    std::function<void(TJobId, EAbortReason)> AbortJobCallback_;
    std::function<void(const TJobletPtr&)> OnSpeculativeJobScheduled_;
    TProgressCounterPtr JobCounter_;

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TIntrusivePtr<TCompetition>> CookieToCompetition_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> SpeculativeCandidates_;

    i64 PendingDataWeight_ = 0;
    int MaxSpeculativeJobCount_ = -1;

    NLogging::TLogger Logger;

    void OnJobFinished(const TJobletPtr& joblet);
    bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
