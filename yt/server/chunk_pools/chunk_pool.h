#pragma once

#include "private.h"

#include <yt/server/controller_agent/helpers.h>
#include <yt/server/controller_agent/progress_counter.h>
#include <yt/server/controller_agent/serialize.h>

#include <yt/server/chunk_server/public.h>

#include <yt/server/scheduler/job.h>
#include <yt/server/scheduler/public.h>

#include <yt/ytlib/chunk_client/input_data_slice.h>
#include <yt/ytlib/chunk_client/public.h>

#include <yt/core/misc/small_vector.h>

namespace NYT {
namespace NChunkPools {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeStatistics
{
    int ChunkCount = 0;
    i64 DataSize = 0;
    i64 RowCount = 0;
    i64 MaxBlockSize = 0;

    void Persist(const TPersistenceContext& context);
};

TChunkStripeStatistics operator + (
    const TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

TChunkStripeStatistics& operator += (
    TChunkStripeStatistics& lhs,
    const TChunkStripeStatistics& rhs);

typedef SmallVector<TChunkStripeStatistics, 1> TChunkStripeStatisticsVector;

//! Adds up input statistics and returns a single-item vector with the sum.
TChunkStripeStatisticsVector AggregateStatistics(
    const TChunkStripeStatisticsVector& statistics);

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe(bool foreign = false, bool solid = false);
    explicit TChunkStripe(NChunkClient::TInputDataSlicePtr dataSlice, bool foreign = false);

    TChunkStripeStatistics GetStatistics() const;
    int GetChunkCount() const;

    int GetTableIndex() const;

    int GetInputStreamIndex() const;

    void Persist(const TPersistenceContext& context);

    SmallVector<NChunkClient::TInputDataSlicePtr, 1> DataSlices;
    int WaitingChunkCount = 0;
    bool Foreign = false;
    bool Solid = false;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList() = default;
    TChunkStripeList(int stripeCount);

    TChunkStripeStatisticsVector GetStatistics() const;
    TChunkStripeStatistics GetAggregateStatistics() const;

    void Persist(const TPersistenceContext& context);

    std::vector<TChunkStripePtr> Stripes;

    TNullable<int> PartitionTag;

    //! If True then TotalDataSize and TotalRowCount are approximate (and are hopefully upper bounds).
    bool IsApproximate = false;

    i64 TotalDataSize = 0;
    i64 LocalDataSize = 0;

    i64 TotalRowCount = 0;

    int TotalChunkCount = 0;
    int LocalChunkCount = 0;
};

DEFINE_REFCOUNTED_TYPE(TChunkStripeList)

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual IPersistent
{
    typedef int TCookie;
    static const TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;

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

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override;

protected:
    bool Finished = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
    : public virtual IPersistent
{
    typedef int TCookie;
    static constexpr TCookie NullCookie = -1;

    virtual i64 GetTotalDataSize() const = 0;
    virtual i64 GetRunningDataSize() const = 0;
    virtual i64 GetCompletedDataSize() const = 0;
    virtual i64 GetPendingDataSize() const = 0;

    virtual i64 GetTotalRowCount() const = 0;

    virtual bool IsCompleted() const = 0;

    virtual int GetTotalJobCount() const = 0;
    virtual int GetPendingJobCount() const = 0;
    virtual const NControllerAgent::TProgressCounter& GetJobCounter() const = 0;

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

    virtual void Completed(TCookie cookie, const NScheduler::TCompletedJobSummary& jobSummary) = 0;
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

    virtual i64 GetTotalDataSize() const override;

    virtual i64 GetRunningDataSize() const override;

    virtual i64 GetCompletedDataSize() const override;

    virtual i64 GetPendingDataSize() const override;

    virtual i64 GetTotalRowCount() const override;

    virtual const NControllerAgent::TProgressCounter& GetJobCounter() const override;

    // IPersistent implementation.

    virtual void Persist(const TPersistenceContext& context) override;

    virtual const std::vector<NChunkClient::TInputChunkPtr>& GetTeleportChunks() const override;

    virtual TOutputOrderPtr GetOutputOrder() const override;

public:
    DEFINE_SIGNAL(void(const TError& error), PoolOutputInvalidated)

protected:
    NControllerAgent::TProgressCounter DataSizeCounter;
    NControllerAgent::TProgressCounter RowCounter;
    NControllerAgent::TProgressCounter JobCounter;

    std::vector<NChunkClient::TInputChunkPtr> TeleportChunks_;
};

////////////////////////////////////////////////////////////////////////////////

class TSuspendableStripe
{
public:
    DEFINE_BYVAL_RW_PROPERTY(IChunkPoolOutput::TCookie, ExtractedCookie);
    DEFINE_BYVAL_RW_PROPERTY(bool, Teleport, false);

public:
    TSuspendableStripe();
    explicit TSuspendableStripe(TChunkStripePtr stripe);

    const TChunkStripePtr& GetStripe() const;
    const TChunkStripeStatistics& GetStatistics() const;
    void Suspend();
    bool IsSuspended() const;
    void Resume(TChunkStripePtr stripe);

    //! Resume chunk and return a hashmap that defines the correspondence between
    //! the old and new chunks. If building such mapping is impossible (for example,
    //! the new stripe contains more data slices, or the new data slices have different
    //! read limits or boundary keys), exception is thrown.
    yhash<NChunkClient::TInputChunkPtr, NChunkClient::TInputChunkPtr> ResumeAndBuildChunkMapping(TChunkStripePtr stripe);

    //! Replaces the original stripe with the current stripe.
    void ReplaceOriginalStripe();

    void Persist(const TPersistenceContext& context);

private:
    TChunkStripePtr Stripe_;
    TChunkStripePtr OriginalStripe_ = nullptr;
    bool Suspended_ = false;
    TChunkStripeStatistics Statistics_;
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

