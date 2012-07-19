#include "stdafx.h"
#include "chunk_pool.h"

#include <ytlib/chunk_holder/chunk_meta_extensions.h>
#include <ytlib/table_client/key.h>

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////

TWeightedChunk::TWeightedChunk()
    : Weight(0)
    , DataWeightOverride(0)
    , RowCountOverride(0)
{ }

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
{ }

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk)
{
    AddChunk(inputChunk);
}

TChunkStripe::TChunkStripe(TRefCountedInputChunkPtr inputChunk, i64 dataWeightOverride, i64 rowCountOverride)
{
    AddChunk(
        inputChunk,
        dataWeightOverride,
        rowCountOverride);
}

void TChunkStripe::AddChunk(TRefCountedInputChunkPtr inputChunk)
{
    AddChunk(
        inputChunk,
        inputChunk->data_weight(),
        inputChunk->row_count());
}

void TChunkStripe::AddChunk(TRefCountedInputChunkPtr inputChunk, i64 dataWeightOverride, i64 rowCountOverride)
{
    Chunks.push_back(TWeightedChunk());
    auto& weightedChunk = Chunks.back();
    
    weightedChunk.InputChunk = inputChunk;
    weightedChunk.DataWeightOverride = dataWeightOverride;
    weightedChunk.RowCountOverride = rowCountOverride;
    // TODO(babenko): make customizable
    weightedChunk.Weight = weightedChunk.DataWeightOverride;
}

std::vector<NChunkServer::TChunkId> TChunkStripe::GetChunkIds() const
{
    std::vector<NChunkServer::TChunkId> result;
    FOREACH (const auto& chunk, Chunks) {
        result.push_back(TChunkId::FromProto(chunk.InputChunk->slice().chunk_id()));
    }
    return result;
}

////////////////////////////////////////////////////////////////////

TPoolExtractionResult::TPoolExtractionResult()
    : TotalChunkWeight(0)
    , TotalChunkCount(0)
    , LocalChunkCount(0)
    , RemoteChunkCount(0)
{ }

void TPoolExtractionResult::AddStripe(TChunkStripePtr stripe, const Stroka& address)
{
    Stripes.push_back(stripe);
    TotalChunkCount += stripe->Chunks.size();
    FOREACH (const auto& chunk, stripe->Chunks) {
        TotalChunkWeight += chunk.Weight;
        const auto& chunkAddresses = chunk.InputChunk->node_addresses();
        if (std::find_if(
            chunkAddresses.begin(),
            chunkAddresses.end(),
            [=] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != chunkAddresses.end())
        {
            ++LocalChunkCount;
        } else {
            ++RemoteChunkCount;
        }
    }
}

void TPoolExtractionResult::AddStripe(TChunkStripePtr stripe)
{
    // NB: Keep this in sync with the above.
    Stripes.push_back(stripe);
    TotalChunkCount += stripe->Chunks.size();
    RemoteChunkCount += stripe->Chunks.size();
    FOREACH (const auto& chunk, stripe->Chunks) {
        TotalChunkWeight += chunk.Weight;
    }
}

////////////////////////////////////////////////////////////////////

class TChunkPoolBase
    : public IChunkPool
{
public:
    virtual const TProgressCounter& WeightCounter() const OVERRIDE
    {
        return WeightCounter_;
    }

    virtual const TProgressCounter& ChunkCounter() const OVERRIDE
    {
        return ChunkCounter_;
    }

    virtual const TProgressCounter& StripeCounter() const OVERRIDE
    {
        return StripeCounter_;
    }

    virtual bool IsCompleted() const OVERRIDE
    {
        return WeightCounter_.GetCompleted() == WeightCounter_.GetTotal();
    }

    virtual bool IsPending() const OVERRIDE
    {
        return WeightCounter_.GetPending() > 0;
    }


protected:
    TProgressCounter WeightCounter_;
    TProgressCounter ChunkCounter_;
    TProgressCounter StripeCounter_;

};

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolBase
{
public:
    explicit TUnorderedChunkPool(bool trackLocality)
        : TrackLocality(trackLocality)
    { }

    virtual void Add(TChunkStripePtr stripe) OVERRIDE
    {
        ChunkCounter_.Increment(stripe->Chunks.size());
        StripeCounter_.Increment(1);

        FOREACH (const auto& chunk, stripe->Chunks) {
            WeightCounter_.Increment(chunk.Weight);
        }

        Register(stripe);
    }

    virtual TPoolExtractionResultPtr Extract(
        const Stroka& address,
        TNullable<i64> weightThreshold) OVERRIDE
    {
        auto result = New<TPoolExtractionResult>();

        if (TrackLocality) {
            // Take local chunks first.
            auto addressIt = LocalChunks.find(address);
            if (addressIt != LocalChunks.end()) {
                const auto& entry = addressIt->second;
                AddAndUnregisterStripes(
                    result,
                    entry.Stripes.begin(),
                    entry.Stripes.end(),
                    weightThreshold,
                    address);
            }
        }

        // Take remote chunks.
        AddAndUnregisterStripes(
            result,
            GlobalChunks.begin(),
            GlobalChunks.end(),
            weightThreshold,
            address);

        WeightCounter_.Start(result->TotalChunkWeight);
        ChunkCounter_.Start(result->TotalChunkCount);
        StripeCounter_.Start(result->Stripes.size());

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result) OVERRIDE
    {
        WeightCounter_.Failed(result->TotalChunkWeight);
        ChunkCounter_.Failed(result->TotalChunkCount);
        StripeCounter_.Failed(result->Stripes.size());

        FOREACH (auto stripe, result->Stripes) {
            Register(stripe);
        }
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result) OVERRIDE
    {
        WeightCounter_.Completed(result->TotalChunkWeight);
        ChunkCounter_.Completed(result->TotalChunkCount);
        StripeCounter_.Completed(result->Stripes.size());
    }

    virtual i64 GetLocality(const Stroka& address) const OVERRIDE
    {
        YASSERT(TrackLocality);
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalWeight;
    }

private:
    bool TrackLocality;

    yhash_set<TChunkStripePtr> GlobalChunks;

    struct TLocalityEntry
    {
        TLocalityEntry()
            : TotalWeight(0)
        { }

        i64 TotalWeight;
        yhash_set<TChunkStripePtr> Stripes;
    };
    
    yhash_map<Stroka,  TLocalityEntry> LocalChunks;

    void Register(TChunkStripePtr stripe)
    {
        if (TrackLocality) {
            FOREACH (const auto& chunk, stripe->Chunks) {
                auto inputChunk = chunk.InputChunk;
                FOREACH (const auto& address, inputChunk->node_addresses()) {
                    auto& entry = LocalChunks[address];
                    YVERIFY(entry.Stripes.insert(stripe).second);
                    entry.TotalWeight += chunk.Weight;
                }
            }
        }

        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    void Unregister(TChunkStripePtr stripe)
    {
        if (TrackLocality) {
            FOREACH (const auto& chunk, stripe->Chunks) {
                auto inputChunk = chunk.InputChunk;
                FOREACH (const auto& address, inputChunk->node_addresses()) {
                    auto& entry = LocalChunks[address];
                    YVERIFY(entry.Stripes.erase(stripe) == 1);
                    entry.TotalWeight -= chunk.Weight;
                }
            }
        }

        YVERIFY(GlobalChunks.erase(stripe) == 1);
    }

    template <class TIterator>
    void AddAndUnregisterStripes(
        TPoolExtractionResultPtr result,
        const TIterator& begin,
        const TIterator& end,
        TNullable<i64> weightThreshold,
        const Stroka& address)
    {
        int oldSize = static_cast<int>(result->Stripes.size());
        for (auto it = begin; it != end; ++it) {
            if (weightThreshold && result->TotalChunkWeight >= weightThreshold.Get()) {
                break;
            }
            if (TrackLocality) {
                result->AddStripe(*it, address);
            } else {
                result->AddStripe(*it);
            }
        }
        int newSize = static_cast<int>(result->Stripes.size());
        for (int index = oldSize; index < newSize; ++index) {
            Unregister(result->Stripes[index]);
        }
    }
};

TAutoPtr<IChunkPool> CreateUnorderedChunkPool(bool trackLocality)
{
    return new TUnorderedChunkPool(trackLocality);
}

////////////////////////////////////////////////////////////////////

class TAtomicChunkPool
    : public TChunkPoolBase
{
public:
    TAtomicChunkPool()
        : Extracted(false)
        , Initialized(false)
    { }

    virtual void Add(TChunkStripePtr stripe) OVERRIDE
    {
        YCHECK(!Initialized);

        ChunkCounter_.Increment(stripe->Chunks.size());
        StripeCounter_.Increment(1);

        Stripes.push_back(stripe);
        
        FOREACH (const auto& chunk, stripe->Chunks) {
            WeightCounter_.Increment(chunk.Weight);
            auto inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk->node_addresses()) {
                AddressToLocality[address] += chunk.Weight;
            }
        }
    }

    virtual TPoolExtractionResultPtr Extract(
        const Stroka& address,
        TNullable<i64> weightThreshold) OVERRIDE
    {
        UNUSED(weightThreshold);

        Initialized = true;
        YCHECK(!Extracted);

        auto result = New<TPoolExtractionResult>();
        FOREACH (auto stripe, Stripes) {
            result->AddStripe(stripe, address);
        }

        Extracted = true;
        WeightCounter_.Start(result->TotalChunkWeight);
        ChunkCounter_.Start(result->TotalChunkCount);
        StripeCounter_.Start(1);

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result) OVERRIDE
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        Extracted = false;
        WeightCounter_.Failed(result->TotalChunkWeight);
        ChunkCounter_.Failed(result->TotalChunkCount);
        StripeCounter_.Failed(result->Stripes.size());
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result) OVERRIDE
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        WeightCounter_.Completed(result->TotalChunkWeight);
        ChunkCounter_.Completed(result->TotalChunkCount);
        StripeCounter_.Completed(result->Stripes.size());
    }

    virtual i64 GetLocality(const Stroka& address) const OVERRIDE
    {
        if (Extracted) {
            return 0;
        }
        auto it = AddressToLocality.find(address);
        return it == AddressToLocality.end() ? 0 : it->second;
    }

private:
    std::vector<TChunkStripePtr> Stripes;

    //! Addresses of added chunks.
    yhash_map<Stroka, i64> AddressToLocality;

    //! Have the stripes been #Extract'ed?
    bool Extracted;

    //! Has any #Extract call been made already?
    bool Initialized;

};

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

