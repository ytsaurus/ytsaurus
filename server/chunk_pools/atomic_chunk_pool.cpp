#include "atomic_chunk_pool.h"

#include "helpers.h"

#include <yt/ytlib/node_tracker_client/public.h>

namespace NYT {
namespace NChunkPools {

using namespace NControllerAgent;
using namespace NNodeTrackerClient;
using namespace NScheduler;

////////////////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
    : public TChunkPoolInputBase
    , public TChunkPoolOutputBase
    , public IChunkPool
    , public NPhoenix::TFactoryTag<NPhoenix::TSimpleFactory>
{
public:
    TAtomicChunkPool()
    {
        JobCounter.Set(1);
    }

    // IChunkPoolInput implementation.

    virtual IChunkPoolInput::TCookie Add(TChunkStripePtr stripe) override
    {
        YCHECK(!Finished);
        YCHECK(!ExtractedList);

        HasPrimaryStripes = HasPrimaryStripes || !stripe->Foreign;

        auto cookie = static_cast<int>(Stripes.size());

        TSuspendableStripe suspendableStripe(stripe);
        Stripes.push_back(suspendableStripe);

        DataWeightCounter.Increment(suspendableStripe.GetStatistics().DataWeight);
        RowCounter.Increment(suspendableStripe.GetStatistics().RowCount);

        return cookie;
    }

    virtual void Finish() override
    {
        if (Finished) {
            JobCounter.Increment(1);
        } else {
            TChunkPoolInputBase::Finish();
        }
    }

    virtual void Suspend(IChunkPoolInput::TCookie cookie) override
    {
        ++SuspendedStripeCount;
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Suspend();
    }

    virtual void Resume(IChunkPoolInput::TCookie cookie, TChunkStripePtr stripe) override
    {
        auto& suspendableStripe = Stripes[cookie];
        suspendableStripe.Resume(stripe);
        --SuspendedStripeCount;
        YCHECK(SuspendedStripeCount >= 0);
    }

    // IChunkPoolOutput implementation.

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override
    {
        TChunkStripeStatisticsVector result;
        result.reserve(Stripes.size());
        for (const auto& suspendableStripe : Stripes) {
            auto stripe = suspendableStripe.GetStripe();
            result.push_back(stripe->GetStatistics());
        }
        return result;
    }

    virtual bool IsCompleted() const override
    {
        return
            Finished &&
            GetPendingJobCount() == 0 &&
            SuspendedStripeCount == 0 &&
            JobCounter.GetRunning() == 0;
    }

    virtual int GetTotalJobCount() const override
    {
        return Finished && HasPrimaryStripes && DataWeightCounter.GetTotal() > 0 ? 1 : 0;
    }

    virtual int GetPendingJobCount() const override
    {
        return
            Finished &&
            SuspendedStripeCount == 0 &&
            DataWeightCounter.GetPending() > 0 &&
            HasPrimaryStripes
            ? 1 : 0;
    }

    virtual i64 GetLocality(TNodeId nodeId) const override
    {
        // Pretend we are local to work around locality timeout.
        return 1;
    }

    virtual IChunkPoolOutput::TCookie Extract(TNodeId nodeId) override
    {
        YCHECK(Finished);
        YCHECK(SuspendedStripeCount == 0);

        if (GetPendingJobCount() == 0) {
            return IChunkPoolOutput::NullCookie;
        }

        ExtractedList = New<TChunkStripeList>();
        for (const auto& suspendableStripe : Stripes) {
            auto stripe = suspendableStripe.GetStripe();
            auto stat = stripe->GetStatistics();
            AddStripeToList(
                stripe,
                stat.DataWeight,
                stat.RowCount,
                ExtractedList,
                nodeId);
        }

        JobCounter.Start(1);
        DataWeightCounter.Start(DataWeightCounter.GetTotal());
        RowCounter.Start(RowCounter.GetTotal());

        return 0;
    }

    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        return ExtractedList;
    }

    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        return ExtractedList->TotalChunkCount;
    }

    virtual void Completed(IChunkPoolOutput::TCookie cookie, const TCompletedJobSummary& jobSummary) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Completed(1, jobSummary.InterruptReason);
        DataWeightCounter.Completed(DataWeightCounter.GetTotal());
        RowCounter.Completed(RowCounter.GetTotal());

        ExtractedList = nullptr;
    }

    virtual void Failed(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Failed(1);
        DataWeightCounter.Failed(DataWeightCounter.GetTotal());
        RowCounter.Failed(RowCounter.GetTotal());

        ExtractedList = nullptr;
    }

    virtual void Aborted(IChunkPoolOutput::TCookie cookie, EAbortReason reason) override
    {
        YCHECK(cookie == 0);
        YCHECK(ExtractedList);
        YCHECK(Finished);

        JobCounter.Aborted(1, reason);
        DataWeightCounter.Aborted(DataWeightCounter.GetTotal(), reason);
        RowCounter.Aborted(RowCounter.GetTotal(), reason);

        ExtractedList = nullptr;
    }

    virtual void Lost(IChunkPoolOutput::TCookie cookie) override
    {
        YCHECK(cookie == 0);
        YCHECK(!ExtractedList);
        YCHECK(Finished);

        JobCounter.Lost(1);
        DataWeightCounter.Lost(DataWeightCounter.GetTotal());
        RowCounter.Lost(RowCounter.GetTotal());
    }

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override
    {
        TChunkPoolInputBase::Persist(context);
        TChunkPoolOutputBase::Persist(context);

        using NYT::Persist;
        Persist(context, Stripes);
        Persist(context, ExtractedList);
        Persist(context, SuspendedStripeCount);
        Persist(context, HasPrimaryStripes);
    }

private:
    DECLARE_DYNAMIC_PHOENIX_TYPE(TAtomicChunkPool, 0x76bac510);

    std::vector<TSuspendableStripe> Stripes;
    TChunkStripeListPtr ExtractedList;
    int SuspendedStripeCount = 0;
    bool HasPrimaryStripes = false;
};

DEFINE_DYNAMIC_PHOENIX_TYPE(TAtomicChunkPool);

std::unique_ptr<IChunkPool> CreateAtomicChunkPool()
{
    return std::unique_ptr<IChunkPool>(new TAtomicChunkPool());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT
