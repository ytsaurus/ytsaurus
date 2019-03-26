#pragma once

#include "private.h"

#include <yt/server/controller_agent/chunk_pools/chunk_pool.h>

namespace NYT::NControllerAgent {

////////////////////////////////////////////////////////////////////////////////

class TCompetitiveJobManager
{
public:
    explicit TCompetitiveJobManager(
        std::function<void(TJobId, EAbortReason)> abortJobCallback,
        const NLogging::TLogger& logger);

    void AddSpeculativeCandidate(const TJobletPtr& joblet);

    int GetPendingSpeculativeJobCount() const;

    int GetTotalSpeculativeJobCount() const;

    NChunkPools::IChunkPoolOutput::TCookie PeekSpeculativeCandidate() const;

    void OnJobScheduled(const TJobletPtr& joblet);
    void OnJobCompleted(const TJobletPtr& joblet);

    // Next two methods return whether we must return cookie to chunk pool or not.
    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason);
    bool OnJobFailed(const TJobletPtr& joblet);

    std::optional<EAbortReason> ShouldAbortJob(const TJobletPtr& joblet) const;

    i64 GetPendingCandidatesDataWeight() const;

    bool IsFinished() const;

    TProgressCounterPtr GetProgressCounter();

    void Persist(const TPersistenceContext& context);

private:
    std::function<void(TJobId, EAbortReason)> AbortJobCallback_;
    TProgressCounterPtr JobCounter_;
    const NLogging::TLogger& Logger;

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, std::vector<TJobId>> CookieToJobIds_;
    THashMap<NChunkPools::IChunkPoolOutput::TCookie, i64> SpeculativeCandidates_;
    THashSet<TJobId> CompetitiveJobLosers_;

    i64 PendingDataWeight_ = 0;

    void OnJobFinished(const TJobletPtr& joblet);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent
