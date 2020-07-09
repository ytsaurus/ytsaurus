#pragma once

#include "private.h"

#include <yt/server/lib/legacy_chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent::NLegacyControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECompetitionStatus,
    (SingleJobOnly)
    (TwoCompetitiveJobs)
    (CompetitionCompleted)
)

class TCompetitiveJobManager
{
public:
    TCompetitiveJobManager(
        std::function<void(const TJobletPtr&)> onSpeculativeJobScheduled,
        std::function<void(TJobId, EAbortReason)> abortJobCallback,
        const NLogging::TLogger& logger,
        int maxSpeculativeJobCount);

    bool TryRegisterSpeculativeCandidate(const TJobletPtr& joblet);

    int GetPendingSpeculativeJobCount() const;

    int GetTotalSpeculativeJobCount() const;

    NLegacyChunkPools::IChunkPoolOutput::TCookie PeekSpeculativeCandidate() const;

    void OnJobScheduled(const TJobletPtr& joblet);
    void OnJobCompleted(const TJobletPtr& joblet);

    // Next two methods return whether we must return cookie to chunk pool or not.
    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason);
    bool OnJobFailed(const TJobletPtr& joblet);

    // If competitive job of this joblet completed we should abort the joblet even if it has completed.
    std::optional<EAbortReason> ShouldAbortJob(const TJobletPtr& joblet) const;

    i64 GetPendingCandidatesDataWeight() const;

    bool IsFinished() const;

    TLegacyProgressCounterPtr GetProgressCounter();

    void Persist(const TPersistenceContext& context);

private:
    struct TCompetition
    {
        ECompetitionStatus Status = ECompetitionStatus::SingleJobOnly;
        std::vector<TJobId> Competitors;
        TJobId JobCompetitionId;
        i64 PendingDataWeight;

        void Persist(const TPersistenceContext& context)
        {
            using NYT::Persist;

            Persist(context, Status);
            Persist(context, Competitors);
            Persist(context, JobCompetitionId);
            Persist(context, PendingDataWeight);
        }
    };

    std::function<void(TJobId, EAbortReason)> AbortJobCallback_;
    std::function<void(const TJobletPtr&)> OnSpeculativeJobScheduled_;
    TLegacyProgressCounterPtr JobCounter_;
    const NLogging::TLogger& Logger;

    THashMap<NLegacyChunkPools::IChunkPoolOutput::TCookie, TCompetition> CookieToCompetition_;
    THashSet<NLegacyChunkPools::IChunkPoolOutput::TCookie> SpeculativeCandidates_;

    i64 PendingDataWeight_ = 0;
    int MaxSpeculativeJobCount_;

    void OnJobFinished(const TJobletPtr& joblet);
    bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(const TLegacyProgressCounterPtr&)>& updateJobCounter);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NLegacyControllers
