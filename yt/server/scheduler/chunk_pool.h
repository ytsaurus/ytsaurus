#pragma once

#include "private.h"
#include "progress_counter.h"
#include "serialize.h"

#include <core/misc/small_vector.h>

#include <ytlib/chunk_client/public.h>
#include <ytlib/chunk_client/chunk_spec.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeStatistics
{
    int ChunkCount = 0;
    i64 DataSize = 0;
    i64 RowCount = 0;
    i64 MaxBlockSize = 0;

    void Persist(TPersistenceContext& context);
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
    TChunkStripe();
    explicit TChunkStripe(NChunkClient::TChunkSlicePtr chunkSlice);
    explicit TChunkStripe(const TChunkStripe& other);

    TChunkStripeStatistics GetStatistics() const;

    void Persist(TPersistenceContext& context);

    SmallVector<NChunkClient::TChunkSlicePtr, 1> ChunkSlices;
    int WaitingChunkCount;

};

DEFINE_REFCOUNTED_TYPE(TChunkStripe)

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeStatisticsVector GetStatistics() const;
    TChunkStripeStatistics GetAggregateStatistics() const;

    void Persist(TPersistenceContext& context);

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

struct IChunkPoolOutput
    : public virtual IPersistent
{
    typedef int TCookie;
    static const TCookie NullCookie = -1;

    virtual i64 GetTotalDataSize() const = 0;
    virtual i64 GetRunningDataSize() const = 0;
    virtual i64 GetCompletedDataSize() const = 0;
    virtual i64 GetPendingDataSize() const = 0;

    virtual i64 GetTotalRowCount() const = 0;

    virtual bool IsCompleted() const = 0;

    virtual int GetTotalJobCount() const = 0;
    virtual int GetPendingJobCount() const = 0;

    //! Approximate average stripe list statistics to estimate memory usage.
    virtual TChunkStripeStatisticsVector GetApproximateStripeStatistics() const = 0;

    virtual i64 GetLocality(NNodeTrackerClient::TNodeId nodeId) const = 0;

    virtual TCookie Extract(NNodeTrackerClient::TNodeId nodeId) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    virtual void Completed(TCookie cookie) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Aborted(TCookie cookie) = 0;
    virtual void Lost(TCookie cookie) = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

std::unique_ptr<IChunkPool> CreateAtomicChunkPool();

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    int jobCount,
    int maxChunkStripesPerJob);

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual IPersistent
{
    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

