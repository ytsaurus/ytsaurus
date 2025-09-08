#pragma once

#include "private.h"

#include <yt/yt/server/lib/chunk_pools/chunk_pool.h>

#include <yt/yt/server/lib/controller_agent/helpers.h>

#include <yt/yt/client/job_tracker_client/public.h>

#include "extra_job_manager.h"

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ECompetitionStatus,
    (SingleJobOnly)
    (TwoCompetitiveJobs)
    (HasCompletedJob)
);

////////////////////////////////////////////////////////////////////////////////

struct ICompetitiveJobManagerHost
    : public IPersistent
{
    virtual void OnSecondaryJobScheduled(const TJobletPtr& joblet, EJobCompetitionType competitionType) = 0;
    virtual void AsyncAbortJob(TJobId jobId, NScheduler::EAbortReason abortReason) = 0;
    virtual void AbortJob(TJobId jobId, NScheduler::EAbortReason abortReason) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TCompetitiveJobManagerBase
    : public IExtraJobManager
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

    i64 GetPendingCandidatesDataWeight() const override;

    int GetPendingJobCount() const override;

    int GetTotalJobCount() const override;

    NChunkPools::IChunkPoolOutput::TCookie PeekJobCandidate() const;

    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason) override;
    bool OnJobFailed(const TJobletPtr& joblet) override;
    void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie) override;

    virtual void OnJobScheduled(const TJobletPtr& joblet) override;

    bool IsRelevant(const TJobletPtr& joblet) const;

    bool IsFinished() const override;

    TProgressCounterPtr GetProgressCounter() const override;

protected:
    struct TCompetition
        : public TRefCounted
    {
        ECompetitionStatus Status = ECompetitionStatus::SingleJobOnly;
        TCompactVector<TJobId, 2> Competitors;
        TJobId JobCompetitionId;
        i64 PendingDataWeight = 0;
        TProgressCounterGuard ProgressCounterGuard;
        bool IsNonTrivial = false;

        TJobId GetCompetitorFor(TJobId jobId);

        PHOENIX_DECLARE_TYPE(TCompetition, 0x9f237b57);
    };

    using TCompetitionPtr = TIntrusivePtr<TCompetition>;

    ICompetitiveJobManagerHost* Host_;

protected:
    NLogging::TSerializableLogger Logger;

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

    i64 PendingDataWeight_ = 0;
    int MaxCompetitiveJobCount_ = -1;

    EJobCompetitionType CompetitionType_;
    EAbortReason ResultLost_;

    //! Return value indicates whether an appropriate cookie must be returned to chunk pool.
    virtual bool OnUnsuccessfulJobFinish(
        const TJobletPtr& joblet,
        const std::function<void(TProgressCounterGuard*)>& updateJobCounter,
        NJobTrackerClient::EJobState state) = 0;

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TCompetitiveJobManagerBase, 0x7f9f0ccb);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers
