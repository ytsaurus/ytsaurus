#include "vanilla_chunk_pool.h"

#include "helpers.h"
#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/structs.h>

#include <yt/yt/ytlib/controller_agent/serialize.h>

#include <yt/yt/core/phoenix/type_decl.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <util/generic/cast.h>

namespace NYT::NChunkPools {

using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TVanillaChunkPool
    : public TChunkPoolOutputWithNewJobManagerBase
    , public TJobSplittingBase
    , public IVanillaChunkPoolOutput
{
public:
    explicit TVanillaChunkPool(const TVanillaChunkPoolOptions& options)
        : TChunkPoolOutputWithNewJobManagerBase(options.Logger)
        , JobCount_(options.JobCount)
        , RestartCompletedJobs_(options.RestartCompletedJobs)
    {
        Logger = options.Logger;

        ValidateLogger(Logger);

        // We use very small portion of job manager functionality. We fill it with dummy
        // jobs and make manager deal with extracting/completing/failing/aborting jobs for us.
        for (int index = 0; index < options.JobCount; ++index) {
            JobManager_->AddJob(std::make_unique<TNewJobStub>());
        }
    }

    //! Used only for persistence.
    TVanillaChunkPool() = default;

    bool IsCompleted() const override
    {
        return
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->JobCounter()->GetPending() == 0;
    }

    void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YT_VERIFY(jobSummary.InterruptionReason != EInterruptReason::JobSplit);

        JobManager_->Completed(cookie, jobSummary.InterruptionReason);
        if (jobSummary.InterruptionReason != EInterruptReason::None || RestartCompletedJobs_) {
            // NB: It is important to lose this job instead of allocating new job since we want
            // to keep range of cookies same as before (without growing infinitely). It is
            // significant to some of the vanilla operation applications like CHYT.
            JobManager_->Lost(cookie);
        }
    }

    void LostAll() override
    {
        YT_LOG_DEBUG("Lost all cookies (Count: %v)", JobCount_);

        for (TOutputCookie cookie = 0; cookie < JobCount_; ++cookie) {
            JobManager_->Lost(cookie, /*force*/ false);
        }
    }

    TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie /*cookie*/) override
    {
        return NullStripeList;
    }

    using TChunkPoolOutputWithNewJobManagerBase::Extract;

    void Extract(IChunkPoolOutput::TCookie cookie) final
    {
        JobManager_->ExtractCookie(cookie);
    }

    void SetJobCount(int jobCount) final
    {
        JobCount_ = jobCount;
    }

private:
    int JobCount_;

    bool RestartCompletedJobs_;

    PHOENIX_DECLARE_POLYMORPHIC_TYPE(TVanillaChunkPool, 0x42439a0a);
};

void TVanillaChunkPool::RegisterMetadata(auto&& registrar)
{
    registrar.template BaseType<TChunkPoolOutputWithJobManagerBase>();
    registrar.template BaseType<TJobSplittingBase>();

    PHOENIX_REGISTER_FIELD(1, RestartCompletedJobs_);
    PHOENIX_REGISTER_FIELD(2, JobCount_);
}

PHOENIX_DEFINE_TYPE(TVanillaChunkPool);

////////////////////////////////////////////////////////////////////////////////

IVanillaChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options)
{
    return New<TVanillaChunkPool>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
