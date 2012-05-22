#pragma once

#include "private.h"

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

class TChunkPool
{
public:
    TChunkPool();
    ~TChunkPool();

    void Add(TChunkStripePtr stripe);

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal);
    void PutBack(TPoolExtractionResultPtr result);

    i64 GetTotalWeight() const;
    i64 GetPendingWeight() const;
    bool HasPendingChunks() const;
    bool HasPendingLocalChunksFor(const Stroka& address) const;

private:
    class TImpl;
    THolder<TImpl> Impl;

};

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
