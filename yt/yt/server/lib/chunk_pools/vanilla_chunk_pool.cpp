#include "vanilla_chunk_pool.h"

#include "new_job_manager.h"

#include <yt/yt/server/lib/controller_agent/serialize.h>
#include <yt/yt/server/lib/controller_agent/structs.h>

#include <library/cpp/yt/memory/ref_tracked.h>

#include <util/generic/cast.h>

namespace NYT::NChunkPools {

using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TVanillaChunkPool
    : public TChunkPoolOutputWithNewJobManagerBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TJobSplittingBase
{
public:
    explicit TVanillaChunkPool(const TVanillaChunkPoolOptions& options)
        : TChunkPoolOutputWithNewJobManagerBase(options.Logger)
        , RestartCompletedJobs_(options.RestartCompletedJobs)
    {
        // We use very small portion of job manager functionality. We fill it with dummy
        // jobs and make manager deal with extracting/completing/failing/aborting jobs for us.
        for (int index = 0; index < options.JobCount; ++index) {
            JobManager_->AddJob(std::make_unique<TNewJobStub>());
        }
    }

    //! Used only for persistence.
    TVanillaChunkPool() = default;

    void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolOutputWithJobManagerBase::Persist(context);

        using NYT::Persist;
        Persist(context, RestartCompletedJobs_);
    }

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
            // NB: it is important to lose this job instead of allocating new job since we want
            // to keep range of cookies same as before (without growing infinitely). It is
            // significant to some of the vanilla operation applications like CHYT.
            JobManager_->Lost(cookie);
        }
    }

    TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie /*cookie*/) override
    {
        return NullStripeList;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool, 0x42439a0a);
    bool RestartCompletedJobs_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool);

////////////////////////////////////////////////////////////////////////////////

IPersistentChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options)
{
    return New<TVanillaChunkPool>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools
