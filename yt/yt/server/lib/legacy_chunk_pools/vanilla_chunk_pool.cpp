#include "vanilla_chunk_pool.h"

#include "job_manager.h"

#include <yt/server/lib/controller_agent/serialize.h>
#include <yt/server/lib/controller_agent/structs.h>

#include <yt/core/misc/ref_tracked.h>

#include <util/generic/cast.h>

namespace NYT::NLegacyChunkPools {

using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TVanillaChunkPool
    : public TChunkPoolOutputWithJobManagerBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    explicit TVanillaChunkPool(const TVanillaChunkPoolOptions& options)
        : RestartCompletedJobs_(options.RestartCompletedJobs)
    {
        // We use very small portion of job manager functionality. We fill it with dummy
        // jobs and make manager deal with extracting/completing/failing/aborting jobs for us.
        for (int index = 0; index < options.JobCount; ++index) {
            JobManager_->AddJob(std::make_unique<TJobStub>());
        }
    }

    //! Used only for persistence.
    TVanillaChunkPool() = default;

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolOutputWithJobManagerBase::Persist(context);

        using NYT::Persist;
        Persist(context, RestartCompletedJobs_);
    }

    virtual bool IsCompleted() const override
    {
        return
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->JobCounter()->GetPending() == 0;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YT_VERIFY(jobSummary.InterruptReason == EInterruptReason::None);
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
        if (RestartCompletedJobs_) {
            // NB: it is important to lose this job intead of alloacting new job since we want
            // to keep range of cookies same as before (without growing infinitely). It is
            // significant to some of the vanilla operation applications like CHYT.
            JobManager_->Lost(cookie);
        }
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        return NullStripeList;
    }

    virtual i64 GetDataSliceCount() const override
    {
        return 0;
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool, 0x42439a0b);
    bool RestartCompletedJobs_;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool);

////////////////////////////////////////////////////////////////////////////////

IChunkPoolOutputPtr CreateVanillaChunkPool(const TVanillaChunkPoolOptions& options)
{
    return New<TVanillaChunkPool>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLegacyChunkPools
