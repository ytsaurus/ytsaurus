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

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList();

    TChunkStripeStatisticsVector GetStatistics() const;
    TChunkStripeStatistics GetAggregateStatistics() const;

    void Persist(TPersistenceContext& context);

    std::vector<TChunkStripePtr> Stripes;

    TNullable<int> PartitionTag;

    //! If True then TotalDataSize and TotalRowCount are approximate (and are hopefully upper bounds).
    bool IsApproximate;

    i64 TotalDataSize;
    i64 LocalDataSize;

    i64 TotalRowCount;

    int TotalChunkCount;
    int LocalChunkCount;

};

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

    virtual i64 GetLocality(const Stroka& address) const = 0;

    virtual TCookie Extract(const Stroka& address) = 0;

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

std::unique_ptr<IChunkPool> CreateAtomicChunkPool(
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory);

std::unique_ptr<IChunkPool> CreateUnorderedChunkPool(
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    int jobCount);

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
    : public virtual IPersistent
{
    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

std::unique_ptr<IShuffleChunkPool> CreateShuffleChunkPool(
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    int partitionCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

