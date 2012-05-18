#pragma once

#include "public.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight)
        : Weight(weight)
    {
        InputChunks.push_back(inputChunk);
    }

    TChunkStripe(const std::vector<NTableClient::NProto::TInputChunk>& inputChunks, i64 weight)
        : Weight(weight)
    {
        InputChunks.insert(InputChunks.end(), inputChunks.begin(), inputChunks.end());
    }

    TSmallVector<NTableClient::NProto::TInputChunk, 1> InputChunks;
    i64 Weight;
};

typedef TIntrusivePtr<TChunkStripe> TChunkStripePtr;

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
{
    virtual ~IChunkPool()
    { }

    virtual void Add(TChunkStripePtr stripe) = 0;

    struct TExtractResult
        : public TIntrinsicRefCounted
    {
        TExtractResult();

        void Add(TChunkStripePtr stripe, const Stroka& address);

        std::vector<TChunkStripePtr> Stripes;
        i64 TotalChunkWeight;
        int TotalChunkCount;
        int LocalChunkCount;
        int RemoteChunkCount;

    };

    typedef TIntrusivePtr<TExtractResult> TExtractResultPtr;

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal) = 0;

    virtual void PutBack(TExtractResultPtr result) = 0;

    virtual i64 GetTotalWeight() const = 0;
    virtual i64 GetPendingWeight() const = 0;
    virtual bool HasPendingChunks() const = 0;
    virtual bool HasPendingLocalChunksFor(const Stroka& address) const = 0;
};

////////////////////////////////////////////////////////////////////

//! Unordered chunk pool may return an arbitrary subset of pooled stripes.
TAutoPtr<IChunkPool> CreateUnorderedChunkPool();

//! Atomic chunk pool always returns all pooled stripes in the order of their insertion.
TAutoPtr<IChunkPool> CreateAtomicChunkPool();

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
