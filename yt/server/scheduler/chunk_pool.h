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

    void GetStatistics(i64* totalDataSize, i64* totalRowCount);

    TSmallVector<NTableClient::TRefCountedInputChunkPtr, 1> Chunks;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripeList
    : public TIntrinsicRefCounted
{
    TChunkStripeList();

    bool TryAddStripe(
        TChunkStripePtr stripe,
        const TNullable<Stroka>& address = Null,
        i64 dataSizeThreshold = std::numeric_limits<i64>::max());

    std::vector<TChunkStripePtr> Stripes;
    
    i64 TotalDataSize;
    i64 TotalRowCount;

    int TotalChunkCount;
    int LocalChunkCount;
    int NonLocalChunkCount;

    // Upper bound on row count in extraction result.
    i64 TotalRowCount;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPoolInput
    : public virtual TIntrinsicRefCounted
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
    : public virtual TIntrinsicRefCounted
{
    virtual ~IChunkPoolOutput()
    { }

    typedef int TCookie;
    static const TCookie NullCookie = -1;

    virtual i64 GetTotalDataSize() const = 0;
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

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(
    int jobCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

struct IShuffleChunkPool
{
    virtual ~IShuffleChunkPool()
    { }

    virtual IChunkPoolInput* GetInput() = 0;
    virtual IChunkPoolOutput* GetOutput(int partitionIndex) = 0;
};

TAutoPtr<IShuffleChunkPool> CreateShuffleChunkPool(
    int partitionCount,
    i64 dataSizeThreshold);

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

