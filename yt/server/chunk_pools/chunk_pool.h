#pragma once

#include "private.h"
#include "chunk_stripe.h"
#include "chunk_stripe_key.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/progress_counter.h>
#include <yt/server/controller_agent/serialize.h>
#include <yt/server/controller_agent/public.h>

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
    virtual void Resume(TCookie cookie) = 0;

    //! When called, pool is forced to replace an input stripe corresponding
    //! to a given cookie with a given new stripe, to apply the given mapping
    //! to the rest of stripes and to form jobs once again.
    virtual void Reset(TCookie cookie, TChunkStripePtr stripe, NControllerAgent::TInputChunkMappingPtr mapping) = 0;

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

    //! This implementation is not ready to go that far.
    virtual void Reset(TCookie cookie, TChunkStripePtr stripe, NControllerAgent::TInputChunkMappingPtr mapping) override;

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

    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const = 0;

    virtual i64 GetDataSliceCount() const = 0;

    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const = 0;

    virtual TOutputOrderPtr GetOutputOrder() const = 0;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const = 0;

    //! Approximate average stripe list statistics to estimate memory usage.
    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    virtual bool IsCompleted() const = 0;

    virtual int GetTotalJobCount() const = 0;
    virtual int GetPendingJobCount() const = 0;

    //! The main purpose of this method is to be much cheaper than #GetStripeList,
    //! and to eliminate creation/desctuction of a stripe list if we have already reached
    //! JobSpecSliceThrottler limit. This is particularly useful for a shuffle chunk pool.
    virtual int GetStripeListSliceCount(TCookie cookie) const = 0;

    virtual void Completed(TCookie cookie, const NScheduler::TCompletedJobSummary& jobSummary) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Aborted(TCookie cookie, NScheduler::EAbortReason reason) = 0;
    virtual void Lost(TCookie cookie) = 0;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IChunkPoolOutput
{
public:
    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const override;

    virtual TOutputOrderPtr GetOutputOrder() const override;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const override;

    virtual void Persist(const TPersistenceContext& context) override;

protected:
    std::vector<NChunkClient::TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputWithCountersBase
    : public TChunkPoolOutputBase
{
public:
    TChunkPoolOutputWithCountersBase();

    virtual i64 GetTotalDataWeight() const override;

    virtual i64 GetRunningDataWeight() const override;

    virtual i64 GetCompletedDataWeight() const override;

    virtual i64 GetPendingDataWeight() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;

    virtual void Persist(const TPersistenceContext& context) override;

protected:
    NControllerAgent::TProgressCounterPtr DataWeightCounter;
    NControllerAgent::TProgressCounterPtr RowCounter;
    NControllerAgent::TProgressCounterPtr JobCounter;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): maybe make job manager implement IChunkPoolOutput itself?
class TChunkPoolOutputWithJobManagerBase
    : public TChunkPoolOutputBase
{
public:
    TChunkPoolOutputWithJobManagerBase();

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override;
    virtual int GetTotalJobCount() const override;
    virtual int GetPendingJobCount() const override;
    virtual IChunkPoolOutput::TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;
    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override;
    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override;
    virtual void Completed(IChunkPoolOutput::TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary) override;
    virtual void Failed(IChunkPoolOutput::TCookie cookie) override;
    virtual void Aborted(IChunkPoolOutput::TCookie cookie, NScheduler::EAbortReason reason) override;
    virtual void Lost(IChunkPoolOutput::TCookie cookie) override;
    virtual i64 GetTotalDataWeight() const override;
    virtual i64 GetRunningDataWeight() const override;
    virtual i64 GetCompletedDataWeight() const override;
    virtual i64 GetPendingDataWeight() const override;
    virtual i64 GetTotalRowCount() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    virtual void Persist(const TPersistenceContext& context) override;

protected:
    TJobManagerPtr JobManager_;
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

