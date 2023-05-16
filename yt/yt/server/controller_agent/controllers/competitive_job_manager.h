#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/client/job_tracker_client/public.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECompetitionStatus,
    (SingleJobOnly)
    (TwoCompetitiveJobs)
    (HasCompletedJob)
);

////////////////////////////////////////////////////////////////////////////////

class ICompetitiveJobManagerHost
    : public IPersistent
{
public:
    virtual void OnSecondaryJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitonType) = 0;
    virtual void AbortJobViaScheduler(TJobId jobId, NScheduler::EAbortReason abortReason) = 0;
    virtual void AbortJobByController(TJobId jobId, NScheduler::EAbortReason abortReason) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCompetitiveJobManagerBase
{
public:
    //! Used only for persistence.
    TCompetitiveJobManagerBase() = default;

    TCompetitiveJobManagerBase(
        ICompetitiveJobManagerHost* host,
        NLogging::TLogger logger,
        int maxSecondaryJobCount,
        EJobCompetitionType competitionType,
        EAbortReason resultLost);

    //! This method may be called either by derived class (in case of probing) or by external code (in case of speculative).
    bool TryAddCompetitiveJob(const TJobletPtr& joblet);

    i64 GetPendingCandidatesDataWeight() const;

    int GetPendingJobCount() const;

    int GetTotalJobCount() const;

    NChunkPools::IChunkPoolOutput::TCookie PeekJobCandidate() const;

    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason);
    bool OnJobFailed(const TJobletPtr& joblet);
    void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie);

    virtual void OnJobScheduled(const TJobletPtr& joblet);
    virtual void OnJobCompleted(const TJobletPtr& joblet) = 0;

    virtual std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) = 0;

    bool IsRelevant(const TJobletPtr& joblet) const;

    bool IsFinished() const;

    TProgressCounterPtr GetProgressCounter() const;

    void Persist(const TPersistenceContext& context);

protected:
    struct TCompetition
        : public TRefCounted
    {
        ECompetitionStatus Status = ECompetitionStatus::SingleJobOnly;
        std::vector<TJobId> Competitors;
        TJobId JobCompetitionId;
        i64 PendingDataWeight = 0;
        TProgressCounterGuard ProgressCounterGuard;
        bool IsNonTrivial = false;

        void Persist(const TPersistenceContext& context);

        TJobId GetCompetitorFor(TJobId jobId);
    };

    using TCompetitionPtr = TIntrusivePtr<TCompetition>;

    ICompetitiveJobManagerHost* Host_;

protected:
    void OnJobFinished(const TJobletPtr& joblet);
    void MarkCompetitionAsCompleted(const TJobletPtr& joblet);
    void BanCookie(NChunkPools::IChunkPoolOutput::TCookie cookie);

    const TCompetitionPtr& GetCompetition(const TJobletPtr& joblet) const;
    TCompetitionPtr FindCompetition(const TJobletPtr& joblet) const;

private:
    TProgressCounterPtr JobCounter_;

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TCompetitionPtr> CookieToCompetition_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> CompetitionCandidates_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> BannedCookies_;

    NLogging::TSerializableLogger Logger;

    i64 PendingDataWeight_ = 0;
    int MaxCompetitiveJobCount_ = -1;

    EJobCompetitionType CompetitionType_;
    EAbortReason ResultLost_;

    //! Return value indicates whether an appropriate cookie must be returned to chunk pool.
    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
        NJobTrackerClient::EJobState state) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
