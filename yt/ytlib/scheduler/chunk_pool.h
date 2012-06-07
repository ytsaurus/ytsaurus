#pragma once

#include "private.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TWeightedChunk
{
    NTableClient::NProto::TInputChunk InputChunk;
    i64 Weight;
};

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe();
    TChunkStripe(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight);

    void AddChunk(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight);

    std::vector<NChunkServer::TChunkId> GetChunkIds() const;

    TSmallVector<TWeightedChunk, 1> Chunks;
    i64 Weight;
};

////////////////////////////////////////////////////////////////////////////////

struct TPoolExtractionResult
    : public TIntrinsicRefCounted
{
    TPoolExtractionResult();

    void Add(TChunkStripePtr stripe, const Stroka& address);

    std::vector<TChunkStripePtr> Stripes;
    i64 TotalChunkWeight;
    int TotalChunkCount;
    int LocalChunkCount;
    int RemoteChunkCount;

};

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
{
    void Add(TChunkStripePtr stripe);

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold = std::numeric_limits<i64>::max());
    void OnFailed(TPoolExtractionResultPtr result);
    void OnCompleted(TPoolExtractionResultPtr result);

    i64 GetTotalWeight() const;
    i64 GetPendingWeight() const;
    i64 GetCompletedWeight() const;

    bool IsCompleted() const;
    bool IsPending() const;
    
    i64 GetLocality(const Stroka& address) const;
};

////////////////////////////////////////////////////////////////////

TAutoPtr<IChunkPool> CreateUnorderedPool();
TAutoPtr<IChunkPool> CreateAtomicPool();

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
