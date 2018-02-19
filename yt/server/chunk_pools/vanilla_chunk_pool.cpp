#include "vanilla_chunk_pool.h"

#include "job_manager.h"

#include <yt/server/controller_agent/operation_controller.h>

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkPools {

using namespace NNodeTrackerClient;
using namespace NScheduler;
using namespace NControllerAgent;

////////////////////////////////////////////////////////////////////////////////

class TVanillaChunkPool
    : public TChunkPoolOutputWithJobManagerBase
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
    , public TRefTracked<TVanillaChunkPool>
{
public:
    explicit TVanillaChunkPool(int jobCount)
    {
        // We use very small portion of job manager functionality. We fill it with dummy
        // jobs and make manager deal with extracting/completing/failing/aborting jobs for us.
        for (int index = 0; index < jobCount; ++index) {
            JobManager_->AddJob(std::make_unique<TJobStub>());
        }
    }

    //! Used only for persistence.
    TVanillaChunkPool() = default;

    virtual void Persist(const TPersistenceContext& context) override
    {
        using NYT::Persist;

        TChunkPoolOutputWithJobManagerBase::Persist(context);
    }

    virtual bool IsCompleted() const override
    {
        return
            JobManager_->JobCounter()->GetRunning() == 0 &&
            JobManager_->JobCounter()->GetPending() == 0;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YCHECK(jobSummary.InterruptReason == EInterruptReason::None);
        JobManager_->Completed(cookie, jobSummary.InterruptReason);
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
    DECLARE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool, 0x42439a0a);
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TVanillaChunkPool);

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IChunkPoolOutput> CreateVanillaChunkPool(int jobCount)
{
    return std::make_unique<TVanillaChunkPool>(jobCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
