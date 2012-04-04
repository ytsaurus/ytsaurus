#pragma once

#include "public.h"

#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TPooledChunk
    : public TIntrinsicRefCounted
{
    TPooledChunk(NTableClient::NProto::TInputChunk& inputChunk, i64 weight)
        : InputChunk(inputChunk)
        , Weight(weight)
    { }

    NTableClient::NProto::TInputChunk InputChunk;
    i64 Weight;
};

typedef TIntrusivePtr<TPooledChunk> TPooledChunkPtr;

////////////////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
{
public:
    void Put(TPooledChunkPtr chunk);
    void Put(const std::vector<TPooledChunkPtr>& chunks);

    void Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal,
        std::vector<TPooledChunkPtr>* extractedChunks,
        i64* extractedWeight,
        int* localCount,
        int* remoteCount);

private:
    yhash_map<Stroka, yhash_set<TPooledChunkPtr> > AddressToChunks;
    yhash_set<TPooledChunkPtr> Chunks;

    void Extract(TPooledChunkPtr chunk);

};

////////////////////////////////////////////////////////////////////

class TMergeChunkPool
{
public:
    void Put(TPooledChunkPtr chunk);
    void Put(const std::vector<TPooledChunkPtr>& chunks);

    void Extract(
        const Stroka& address,
        int maxCount,
        bool needLocal,
        std::vector<TPooledChunkPtr>* extractedChunks,
        i64* extractedWeight,
        int* localCount,
        int* remoteCount);

private:

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
