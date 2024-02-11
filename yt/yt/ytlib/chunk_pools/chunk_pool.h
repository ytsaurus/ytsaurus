#pragma once

#include "private.h"
#include "chunk_stripe.h"
#include "chunk_stripe_key.h"

#include <yt/yt/ytlib/chunk_client/public.h>

#include <yt/yt/ytlib/controller_agent/public.h>

#include <yt/yt/core/actions/signal.h>

#include <yt/yt/core/logging/logger_owner.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

namespace NYT::NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual TRefCounted
{
    using TCookie = TInputCookie;
    static constexpr TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;

    virtual TCookie AddWithKey(TChunkStripePtr stripe, TChunkStripeKey /*key*/)
    {
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

//! This interface is a chunk pool counterpart for a job splitter.
struct IChunkPoolJobSplittingHost
    : public virtual TRefCounted
{
    //! Returns true if a job corresponding to this cookie may be considered for splitting and false otherwise.
    virtual bool IsSplittable(TOutputCookie cookie) const = 0;
};

DEFINE_REFCOUNTED_TYPE(IChunkPoolJobSplittingHost)

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
    : public virtual TRefCounted
    , public virtual IChunkPoolJobSplittingHost
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
    virtual NTableClient::TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

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
    DECLARE_INTERFACE_SIGNAL(void(NChunkClient::TInputChunkPtr, std::any tag), ChunkTeleported);

    //! Raises when chunk pool completes.
    DECLARE_INTERFACE_SIGNAL(void(), Completed);
    //! Raises when chunk pool uncompletes.
    DECLARE_INTERFACE_SIGNAL(void(), Uncompleted);
};

DEFINE_REFCOUNTED_TYPE(IChunkPoolOutput)

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

DEFINE_REFCOUNTED_TYPE(IChunkPool)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkPools

