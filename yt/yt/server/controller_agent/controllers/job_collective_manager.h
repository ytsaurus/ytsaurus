#pragma once

#include "competitive_job_manager.h"

#include <util/generic/vector.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Multiple jobs are created when CollectiveOptions.Size is > 1.
//! The cookie collective counts as completed when all the jobs successfully complete.
//! When some jobs fail or abort, then it gets restarted.

class TJobCollectiveManager
    : public IExtraJobManager
{
public:
    //! Used only for persistence.
    TJobCollectiveManager() = default;

    TJobCollectiveManager(
        TTask* host,
        NLogging::TLogger logger);

    i64 GetPendingCandidatesDataWeight() const final;

    int GetPendingJobCount() const override final;

    int GetTotalJobCount() const override final;

    std::pair<NChunkPools::IChunkPoolOutput::TCookie, int> PeekJobCandidate();

    bool OnJobAborted(const TJobletPtr& joblet, EAbortReason reason) override final;
    bool OnJobFailed(const TJobletPtr& joblet) override final;
    void OnJobLost(NChunkPools::IChunkPoolOutput::TCookie cookie) override final;

    void OnJobScheduled(const TJobletPtr& joblet) override final;
    void OnOperationRevived() override final;
    bool OnJobCompleted(const TJobletPtr& joblet) override final;

    std::optional<EAbortReason> ShouldAbortCompletingJob(const TJobletPtr& joblet) override final;

    bool IsFinished() const override final;

    TProgressCounterPtr GetProgressCounter() const override final;

    int GetCollectiveSize() const;

    void InitializeCounter();

private:
    struct TSlave
    {
        TJobId JobId;
        TProgressCounterGuard ProgressCounterGuard;

        PHOENIX_DECLARE_TYPE(TSlave, 0x9f237b98);
    };

    struct TCollective
    {
        TJobId MasterJobId;
        TCompactVector<TSlave, 3> Slaves;
        int Pending = 0;

        PHOENIX_DECLARE_TYPE(TCollective, 0x9f237b97);
    };

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TCollective> CookieToCollective_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> PendingCookies_;
    TProgressCounterPtr JobCounter_;

    TTask* Task_ = nullptr;

    NLogging::TSerializableLogger Logger;

    bool IsRelevant() const;

    bool OnUnsuccessfulJobFinish(const TJobletPtr& joblet);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TJobCollectiveManager, 0xccfb1994);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

