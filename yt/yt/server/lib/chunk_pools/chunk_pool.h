#pragma once

#include "private.h"
#include "chunk_stripe.h"
#include "chunk_stripe_key.h"

#include <yt/yt/server/lib/controller_agent/progress_counter.h>

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/misc/small_vector.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual TRefCounted
    , public virtual IPersistent
{
    using TCookie = TInputCookie;
    static const TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey /* key */) {
        return Add(stripe);
    }

    virtual void Suspend(TCookie cookie) = 0;
    virtual void Resume(TCookie cookie) = 0;

    //! When called, pool is forced to replace an input stripe corresponding
    //! to a given cookie with a given new stripe, to apply the given mapping
    //! to the rest of stripes and to form jobs once again.
    virtual void Reset(TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) = 0;

    virtual void Finish() = 0;

    virtual bool IsFinished() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkPoolInput)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPoolInput
    : public virtual IChunkPoolInput
{
    //! Finishes underlying pool with given index.
    //! NB: One should not finish underlying pools directlty.
    //! For now, this method is used for testing purposes only.
    virtual void FinishPool(int poolIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPoolInput)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolInputBase
    : public virtual IChunkPoolInput
{
public:
    // IChunkPoolInput implementation.
    virtual void Finish() override;

    virtual bool IsFinished() const override;

    //! This implementation checks that key is not set (that is true for all standard
    //! chunk pools) and that `stripe` contains data slices, after that it
    //! forwards the call to the internal `Add` method.
    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey key) override;

    //! This implementation is not ready to go that far.
    virtual void Reset(TCookie cookie, TChunkStripePtr stripe, TInputChunkMappingPtr mapping) override;

    // IPersistent implementation.
    virtual void Persist(const TPersistenceContext& context) override;

protected:
    bool Finished = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
    : public virtual TRefCounted
    , public virtual IPersistent
{
    using TCookie = TOutputCookie;
    static constexpr TCookie NullCookie = -1;

    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const = 0;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const = 0;
    virtual const NControllerAgent::TProgressCounterPtr& GetRowCounter() const = 0;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const = 0;

    virtual TOutputOrderPtr GetOutputOrder() const = 0;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const = 0;

    //! Approximate average stripe list statistics to estimate memory usage.
    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

    virtual TCookie Extract(
        NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    virtual bool IsCompleted() const = 0;

    //! The main purpose of this method is to be much cheaper than #GetStripeList,
    //! and to eliminate creation/desctuction of a stripe list if we have already reached
    //! JobSpecSliceThrottler limit. This is particularly useful for a shuffle chunk pool.
    virtual int GetStripeListSliceCount(TCookie cookie) const = 0;

    virtual void Completed(TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Aborted(TCookie cookie, NScheduler::EAbortReason reason) = 0;
    virtual void Lost(TCookie cookie) = 0;

    //! Raises when chunk teleports.
    DEFINE_SIGNAL(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);

    //! Raises when chunk pool completes.
    DEFINE_SIGNAL(void(), Completed);
    //! Raises when chunk pool uncompletes.
    DEFINE_SIGNAL(void(), Uncompleted);
};

DEFINE_REFCOUNTED_TYPE(IChunkPoolOutput)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPoolOutput
    : public virtual IChunkPoolOutput
{
    //! Should be called when all underlying pools are added.
    virtual void Finalize() = 0;

    //! Adds new underlying chunk pool output to multi chunk pool.
    virtual void AddPoolOutput(IChunkPoolOutputPtr pool, int poolIndex) = 0;

    //! Extracts cookie from underlying pool `underlyingPoolIndexHint' if possible.
    virtual TCookie ExtractFromPool(
        int underlyingPoolIndexHint,
        NNodeTrackerClient::TNodeId nodeId = NNodeTrackerClient::InvalidNodeId) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPoolOutput)

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputBase
    : public virtual IChunkPoolOutput
{
public:
    virtual TOutputOrderPtr GetOutputOrder() const override;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const override;
};

////////////////////////////////////////////////////////////////////////////////

class TChunkPoolOutputWithCountersBase
    : public TChunkPoolOutputBase
{
public:
    TChunkPoolOutputWithCountersBase();

    virtual void Persist(const TPersistenceContext& context) override;

    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetRowCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const override;

protected:
    NControllerAgent::TProgressCounterPtr DataWeightCounter;
    NControllerAgent::TProgressCounterPtr RowCounter;
    NControllerAgent::TProgressCounterPtr JobCounter;
    NControllerAgent::TProgressCounterPtr DataSliceCounter;
};

////////////////////////////////////////////////////////////////////////////////

// TODO(max42): maybe make job manager implement IChunkPoolOutput itself?
template <class TJobManager>
class TChunkPoolOutputWithJobManagerBase
    : public TChunkPoolOutputBase
{
public:
    //! Used only for persistence.
    TChunkPoolOutputWithJobManagerBase() = default;

    TChunkPoolOutputWithJobManagerBase(const NLogging::TLogger& logger);

    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const override;
    virtual IChunkPoolOutput::TCookie Extract(NNodeTrackerClient::TNodeId nodeId) override;
    virtual TChunkStripeListPtr GetStripeList(IChunkPoolOutput::TCookie cookie) override;
    virtual int GetStripeListSliceCount(IChunkPoolOutput::TCookie cookie) const override;
    virtual void Completed(IChunkPoolOutput::TCookie cookie, const NControllerAgent::TCompletedJobSummary& jobSummary) override;
    virtual void Failed(IChunkPoolOutput::TCookie cookie) override;
    virtual void Aborted(IChunkPoolOutput::TCookie cookie, NScheduler::EAbortReason reason) override;
    virtual void Lost(IChunkPoolOutput::TCookie cookie) override;
    virtual const NControllerAgent::TProgressCounterPtr& GetJobCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataWeightCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetRowCounter() const override;
    virtual const NControllerAgent::TProgressCounterPtr& GetDataSliceCounter() const override;

    virtual void Persist(const TPersistenceContext& context) override;

protected:
    TIntrusivePtr<TJobManager> JobManager_;
};

using TChunkPoolOutputWithLegacyJobManagerBase = TChunkPoolOutputWithJobManagerBase<TLegacyJobManager>;
using TChunkPoolOutputWithNewJobManagerBase = TChunkPoolOutputWithJobManagerBase<TNewJobManager>;

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

DEFINE_REFCOUNTED_TYPE(IChunkPool)

////////////////////////////////////////////////////////////////////////////////

struct IMultiChunkPool
    : public virtual IMultiChunkPoolInput
    , public virtual IMultiChunkPoolOutput
    , public virtual IChunkPool
{
    //! Adds new underlying chunk pool to multi chunk pool.
    virtual void AddPool(IChunkPoolPtr pool, int poolIndex) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMultiChunkPool)

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual TRefCounted
    , public virtual IPersistent
{
    virtual IChunkPoolInputPtr GetInput() = 0;
    virtual IChunkPoolOutputPtr GetOutput(int partitionIndex) = 0;
    virtual i64 GetTotalDataSliceCount() const = 0;
    virtual i64 GetTotalJobCount() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IShuffleChunkPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

