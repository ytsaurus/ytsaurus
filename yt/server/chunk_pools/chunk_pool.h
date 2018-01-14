#pragma once

#include "private.h"
#include "chunk_stripe.h"
#include "chunk_stripe_key.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/progress_counter.h>
#include <yt/server/controller_agent/serialize.h>

#include <yt/server/chunk_server/public.h>

#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/public.h>

#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual IPersistent
{
    using TCookie = TIntCookie;
    static const TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) {
        return Add(stripe);
    }

    virtual void Suspend(TCookie cookie) = 0;
    virtual void Resume(TCookie cookie, TChunkStripePtr stripe) = 0;
    virtual void Finish() = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputBase
    : public virtual IChunkPoolInput
{
public:
    // IChunkPoolInput implementation.
    virtual void Finish() override;

    //! This implementation checks that key is not set (that is true for all standard
    //! chunk pools) and that `stripe` contains data slices, after that it
    //! forwards the call to the internal `Add` method.
    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override;

    // IPersistent implementation.
    virtual void Persist(const TPersistenceContext& context) override;

protected:
    bool Finished = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
    : public virtual IPersistent
{
    using TCookie = TIntCookie;
    static constexpr TCookie NullCookie = -1;

    virtual i64 GetTotalDataWeight() const = 0;
    virtual i64 GetRunningDataWeight() const = 0;
    virtual i64 GetCompletedDataWeight() const = 0;
    virtual i64 GetPendingDataWeight() const = 0;

    virtual i64 GetTotalRowCount() const = 0;

    virtual bool IsCompleted() const = 0;

    virtual int GetTotalJobCount() const = 0;
    virtual int GetPendingJobCount() const = 0;
    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const = 0;

    virtual i64 GetDataSliceCount() const = 0;

    //! Approximate average stripe list statistics to estimate memory usage.
    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    //! The main purpose of this method is to be much cheaper than #GetStripeList,
    //! and to eliminate creation/desctuction of a stripe list if we have already reached
    //! JobSpecSliceThrottler limit. This is particularly useful for a shuffle chunk pool.
    virtual int GetStripeListSliceCount(TCookie cookie) const = 0;

    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const = 0;

    virtual TOutputOrderPtr GetOutputOrder() const = 0;

    virtual void Completed(TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Aborted(TCookie cookie, NScheduler::EAbortReason reason) = 0;
    virtual void Lost(TCookie cookie) = 0;

    //! Raised when all the output cookies from this pool no longer correspond to valid jobs.
    DECLARE_INTERFACE_SIGNAL(void(const TError& error), PoolOutputInvalidated);
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IChunkPoolOutput
{
public:
    TChunkPoolOutputBase();

    // IChunkPoolOutput implementation.
    virtual i64 GetTotalDataWeight() const override;
    virtual i64 GetRunningDataWeight() const override;
    virtual i64 GetCompletedDataWeight() const override;
    virtual i64 GetPendingDataWeight() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const override;
    virtual TOutputOrderPtr GetOutputOrder() const override;

    // IPersistent implementation.
    virtual void Persist(const TPersistenceContext& context) override;

public:
    DEFINE_SIGNAL(void(const TError& error), PoolOutputInvalidated)

protected:
    NControllerAgent::TProgressCounterPtr DataWeightCounter;
    NControllerAgent::TProgressCounterPtr RowCounter;
    NControllerAgent::TProgressCounterPtr JobCounter;

    std::vector<NChunkClient::TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual IPersistent
{
    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NChunkPools
} // namespace NYT

