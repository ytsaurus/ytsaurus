#pragma once

#include "competitive_job_manager.h"

#include <util/generic/vector.h>

namespace NYT::NControllerAgent::NControllers {

////////////////////////////////////////////////////////////////////////////////

//! Multiple jobs are created when CollectiveOptions.Size is > 1.
//! The cookie collective counts as completed when the master job completes before the slaves.
//! When some jobs fail or abort and the master job is not completed yet, then the collective gets restarted.
//! When the master job completes and there are running slaves, then we wait for them to finish,
//! after that the collective is removed from the state.
//! Slaves may complete or abort or fail, but the collective is considered completed when the master job completes.
//! When the master job completes, from the chunk pool point of view, the job is considered completed.

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
        // Empty jobId means that the slave is not running.
        TJobId JobId;
        TProgressCounterGuard ProgressCounterGuard;

        PHOENIX_DECLARE_TYPE(TSlave, 0x9f237b98);
    };

    struct TCollective
    {
        TJobId MasterJobId;
        TCompactVector<TSlave, 3> Slaves;
        int Pending = 0;
        bool Finished = false;

        bool HasRunningSlaves() const;

        PHOENIX_DECLARE_TYPE(TCollective, 0x9f237b97);
    };

    THashMap<NChunkPools::IChunkPoolOutput::TCookie, TCollective> CookieToCollective_;
    THashSet<NChunkPools::IChunkPoolOutput::TCookie> PendingCookies_;
    TProgressCounterPtr JobCounter_;

    TTask* Task_ = nullptr;

    NLogging::TSerializableLogger Logger;

    bool IsRelevant() const;

    bool OnUnsuccessfulJobFinish(const TJobletPtr& joblet, EAbortReason abortReason = EAbortReason::None);

    PHOENIX_DECLARE_FRIEND();
    PHOENIX_DECLARE_TYPE(TJobCollectiveManager, 0xccfb1994);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NControllerAgent::NControllers

