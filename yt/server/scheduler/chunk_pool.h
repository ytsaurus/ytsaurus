#pragma once

#include "private.h"
#include "progress_counter.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/table_client/table_reader.pb.h>

#include <server/chunk_server/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe();
    explicit TChunkStripe(NTableClient::TRefCountedInputChunkPtr inputChunk);
    explicit TChunkStripe(const TChunkStripe& other);

    TSmallVector<NTableClient::TRefCountedInputChunkPtr, 1> Chunks;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList();

    std::vector<TChunkStripePtr> Stripes;
    
    TNullable<int> PartitionTag;

    i64 TotalDataSize;
    i64 TotalRowCount;

    int TotalChunkCount;
    int LocalChunkCount;
    int NonLocalChunkCount;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
{
    virtual ~IChunkPoolInput()
    { }

    typedef int TCookie;
    static const TCookie NullCookie = -1;

    virtual TCookie Add(TChunkStripePtr stripe) = 0;
    virtual int GetTotalStripeCount() const = 0;

    virtual void Suspend(TCookie cookie) = 0;
    virtual bool Resume(TCookie cookie, TChunkStripePtr stripe) = 0;
    virtual void Finish() = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolOutput
{
    virtual ~IChunkPoolOutput()
    { }

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

    virtual i64 GetLocality(const Stroka& address) const = 0;

    virtual TCookie Extract(const Stroka& address) = 0;

    virtual TChunkStripeListPtr GetStripeList(TCookie cookie) = 0;

    virtual void Completed(TCookie cookie) = 0;
    virtual void Failed(TCookie cookie) = 0;
    virtual void Lost(TCookie cookie) = 0;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
    : public virtual IChunkPoolInput
    , public virtual IChunkPoolOutput
{ };

TAutoPtr<IChunkPool> CreateAtomicChunkPool();

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(int jobCount);

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
{
    virtual ~IShuffleChunkPool()
    { }

    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

TAutoPtr<IShuffleChunkPool> CreateShuffleChunkPool(
    const std::vector<i64>& dataSizeThresholds);

////////////////////////////////////////////////////////////////////////////////

void GetStatistics(
    const TChunkStripePtr& stripe,
    i64* totalDataSize = NULL,
    i64* totalRowCount = NULL);

bool TryAddStripeToList(
    const TChunkStripePtr& stripe,
    const TChunkStripeListPtr& list,
    const TNullable<Stroka>& address = Null,
    i64 dataSizeThreshold = std::numeric_limits<i64>::max());

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

