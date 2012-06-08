#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

using namespace NChunkServer;
using namespace NTableClient::NProto;

////////////////////////////////////////////////////////////////////

TChunkStripe::TChunkStripe()
    : Weight(0)
{ }

TChunkStripe::TChunkStripe(const TInputChunk& inputChunk, i64 weight)
    : Weight(0)
{
    AddChunk(inputChunk, weight);
}

void TChunkStripe::AddChunk(const TInputChunk& inputChunk, i64 weight)
{
    Chunks.push_back(TWeightedChunk());
    auto& stripeChunk = Chunks.back();
    stripeChunk.InputChunk = inputChunk;
    stripeChunk.Weight = weight;
    Weight += weight;
}

std::vector<NChunkServer::TChunkId> TChunkStripe::GetChunkIds() const
{
    std::vector<NChunkServer::TChunkId> result;
    FOREACH (const auto& chunk, Chunks) {
        result.push_back(TChunkId::FromProto(chunk.InputChunk.slice().chunk_id()));
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

void TPoolExtractionResult::Add(TChunkStripePtr stripe, const Stroka& address)
{
    Stripes.push_back(stripe);
    TotalChunkWeight += stripe->Weight;
    FOREACH (const auto& chunk, stripe->Chunks) {
        const auto& inputChunk = chunk.InputChunk;
        ++TotalChunkCount;
        if (std::find_if(
            inputChunk.node_addresses().begin(),
            inputChunk.node_addresses().end(),
            [&] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != inputChunk.node_addresses().end())
        {
            ++LocalChunkCount;
        } else {
            ++RemoteChunkCount;
        }
    }
}

////////////////////////////////////////////////////////////////////

class TChunkPoolBase
    : public IChunkPool
{
public:
    virtual const TProgressCounter& WeightCounter() const
    {
        return WeightCounter_;
    }

    virtual const TProgressCounter& ChunkCounter() const
    {
        return ChunkCounter_;
    }

    virtual bool IsCompleted() const
    {
        return WeightCounter_.GetCompleted() == WeightCounter_.GetTotal();
    }

    virtual bool IsPending() const
    {
        return WeightCounter_.GetPending() > 0;
    }


protected:
    TProgressCounter WeightCounter_;
    TProgressCounter ChunkCounter_;

};

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public TChunkPoolBase
{
public:
    virtual void Add(TChunkStripePtr stripe)
    {
        YCHECK(stripe->Weight > 0);

        WeightCounter_.Increment(stripe->Weight);
        ChunkCounter_.Increment(stripe->Chunks.size());

        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                auto& entry = LocalChunks[address];
                YVERIFY(entry.Stripes.insert(stripe).second);
                entry.TotalWeight += chunk.Weight;
            }
        }

        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    virtual TPoolExtractionResultPtr Extract(
        const Stroka& address,
        TNullable<i64> weightThreshold)
    {
        auto result = New<TPoolExtractionResult>();

        // Take local chunks first.
        auto addressIt = LocalChunks.find(address);
        if (addressIt != LocalChunks.end()) {
            const auto& entry = addressIt->second;
            AddStripes(
                result,
                entry.Stripes.begin(),
                entry.Stripes.end(),
                weightThreshold,
                address);
        }

        // Unregister taken local chunks.
        // We have to do this right away, otherwise we risk getting same chunks
        // in the next phase.
        for (int index = 0; index < result->LocalChunkCount; ++index) {
            Unregister(result->Stripes[index]);
        }

        // Take remote chunks.
        AddStripes(
            result,
            GlobalChunks.begin(),
            GlobalChunks.end(),
            weightThreshold,
            address);

        // Unregister taken remote chunks.
        for (int index = result->LocalChunkCount; index < result->LocalChunkCount + result->RemoteChunkCount; ++index) {
            Unregister(result->Stripes[index]);
        }

        WeightCounter_.Start(result->TotalChunkWeight);
        ChunkCounter_.Start(result->TotalChunkCount);

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result)
    {
        WeightCounter_.Failed(result->TotalChunkWeight);
        ChunkCounter_.Failed(result->TotalChunkCount);

        FOREACH (const auto& stripe, result->Stripes) {
            Add(stripe);
        }
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result)
    {
        WeightCounter_.Completed(result->TotalChunkWeight);
        ChunkCounter_.Completed(result->TotalChunkCount);
    }

    virtual i64 GetLocality(const Stroka& address) const
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalWeight;
    }

private:
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

    void Unregister(TChunkStripePtr stripe)
    {
        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                auto& entry = LocalChunks[address];
                YVERIFY(entry.Stripes.erase(stripe) == 1);
                entry.TotalWeight -= chunk.Weight;
            }
        }

        YVERIFY(GlobalChunks.erase(stripe) == 1);
    }

    template <class TIterator>
    void AddStripes(
        TPoolExtractionResultPtr result,
        const TIterator& begin,
        const TIterator& end,
        TNullable<i64> weightThreshold,
        const Stroka& address)
    {
        for (auto it = begin; it != end; ++it) {
            if (weightThreshold && result->TotalChunkWeight >= weightThreshold.Get()) {
                break;
            }
            result->Add(*it, address);
        }
    }
};

TAutoPtr<IChunkPool> CreateUnorderedChunkPool()
{
    return new TUnorderedChunkPool();
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

    virtual void Add(TChunkStripePtr stripe)
    {
        YCHECK(!Initialized);

        WeightCounter_.Increment(stripe->Weight);
        ChunkCounter_.Increment(stripe->Chunks.size());

        Stripes.push_back(stripe);
        
        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                AddressToLocality[address] += chunk.Weight;
            }
        }
    }

    virtual TPoolExtractionResultPtr Extract(const Stroka& address, TNullable<i64> weightThreshold)
    {
        UNUSED(weightThreshold);

        Initialized = true;
        YCHECK(!Extracted);

        auto result = New<TPoolExtractionResult>();
        FOREACH (const auto& stripe, Stripes) {
            result->Add(stripe, address);
        }

        Extracted = true;
        WeightCounter_.Start(result->TotalChunkWeight);
        ChunkCounter_.Start(result->TotalChunkCount);

        return result;
    }

    virtual void OnFailed(TPoolExtractionResultPtr result)
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        Extracted = false;
        WeightCounter_.Failed(result->TotalChunkWeight);
        ChunkCounter_.Failed(result->TotalChunkCount);
    }

    virtual void OnCompleted(TPoolExtractionResultPtr result)
    {
        YCHECK(Initialized);
        YCHECK(Extracted);

        WeightCounter_.Completed(result->TotalChunkWeight);
        ChunkCounter_.Completed(result->TotalChunkCount);
    }

    virtual i64 GetLocality(const Stroka& address) const
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

