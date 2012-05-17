#pragma once

#include "public.h"

#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TPooledChunk
    : public TIntrinsicRefCounted
{
    TPooledChunk(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight)
        : InputChunk(inputChunk)
        , Weight(weight)
    { }

    NTableClient::NProto::TInputChunk InputChunk;
    i64 Weight;
};

typedef TIntrusivePtr<TPooledChunk> TPooledChunkPtr;

////////////////////////////////////////////////////////////////////////////////

struct IChunkPool
{
    virtual ~IChunkPool()
    { }

    virtual void Add(TPooledChunkPtr chunk) = 0;

    struct TExtractResult
        : public TIntrinsicRefCounted
    {
        TExtractResult()
            : Weight(0)
            , LocalCount(0)
            , RemoteCount(0)
        { }


        void Add(TPooledChunkPtr chunk, bool local)
        {
            Chunks.push_back(chunk);
            Weight += chunk->Weight;
            if (local) {
                ++LocalCount;
            } else {
                ++RemoteCount;
            }
        }

        
        std::vector<TPooledChunkPtr> Chunks;
        i64 Weight;
        int LocalCount;
        int RemoteCount;

    };

    typedef TIntrusivePtr<TExtractResult> TExtractResultPtr;

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal) = 0;

    virtual void PutBack(TExtractResultPtr result) = 0;

    virtual i64 GetTotalWeight() const = 0;
    virtual i64 GetPendingWeight() const = 0;
    virtual bool HasPendingChunks() const = 0;
    virtual bool HasPendingLocalChunksFor(const Stroka& address) const = 0;
};

////////////////////////////////////////////////////////////////////

//! Unordered chunk pool may return an arbitrary subset of pooled chunks.
TAutoPtr<IChunkPool> CreateUnorderedChunkPool();

//! Atomic chunk pool always returns all pooled chunks in the order of their insertion.
TAutoPtr<IChunkPool> CreateAtomicChunkPool();

//! Merge chunk pool tries to ensure that the weights of returned chunks are close.
TAutoPtr<IChunkPool> CreateMergeChunkPool();

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
