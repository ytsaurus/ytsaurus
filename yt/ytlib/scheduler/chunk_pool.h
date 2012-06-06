#pragma once

#include "private.h"

#include <ytlib/misc/small_vector.h>
#include <ytlib/chunk_server/public.h>
#include <ytlib/table_client/table_reader.pb.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

struct TChunkStripe
    : public TIntrinsicRefCounted
{
    TChunkStripe();
    TChunkStripe(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight);
    TChunkStripe(const std::vector<NTableClient::NProto::TInputChunk>& inputChunks, i64 weight);

    void AddChunk(const NTableClient::NProto::TInputChunk& inputChunk, i64 weight);

    std::vector<NChunkServer::TChunkId> GetChunkIds() const;

    TSmallVector<NTableClient::NProto::TInputChunk, 1> InputChunks;
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

class TUnorderedChunkPool
{
public:
    TUnorderedChunkPool();
    ~TUnorderedChunkPool();

    void Add(TChunkStripePtr stripe);

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal);
    void Failed(TPoolExtractionResultPtr result);
    void Completed(TPoolExtractionResultPtr result);

    i64 GetTotalWeight() const;
    i64 GetPendingWeight() const;
    i64 GetCompletedWeight() const;

    bool IsCompleted() const;
    bool HasPendingChunks() const;
    bool HasPendingLocalChunksAt(const Stroka& address) const;

private:
    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
{
public:
    TAtomicChunkPool();
    ~TAtomicChunkPool();

    void Add(TChunkStripePtr stripe);

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        bool needLocal);
    void Failed(TPoolExtractionResultPtr result);
    void Completed(TPoolExtractionResultPtr result);

    i64 GetTotalWeight() const;
    i64 GetPendingWeight() const;
    i64 GetCompletedWeight() const;

    bool IsCompleted() const;
    bool HasPendingChunks() const;
    bool HasPendingLocalChunksAt(const Stroka& address) const;
     
private:
    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
