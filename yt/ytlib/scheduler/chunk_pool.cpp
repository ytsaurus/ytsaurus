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

class TUnorderedChunkPool
    : public IChunkPool
{
public:
    TUnorderedChunkPool()
        : TotalWeight(0)
        , PendingWeight(0)
        , CompletedWeight(0)
    { }

    void Add(TChunkStripePtr stripe)
    {
        YASSERT(stripe->Weight > 0);
        TotalWeight += stripe->Weight;
        PendingWeight += stripe->Weight;
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

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold)
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

        return result;
    }

    void OnFailed(TPoolExtractionResultPtr result)
    {
        FOREACH (const auto& stripe, result->Stripes) {
            Add(stripe);
        }
    }

    void OnCompleted(TPoolExtractionResultPtr result)
    {
        CompletedWeight += result->TotalChunkWeight;
    }

    i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    i64 GetPendingWeight() const
    {
        return PendingWeight;
    }

    i64 GetCompletedWeight() const
    {
        return CompletedWeight;
    }

    bool IsCompleted() const
    {
        return CompletedWeight == TotalWeight;
    }

    bool IsPending() const
    {
        return !GlobalChunks.empty();
    }

    i64 GetLocality(const Stroka& address) const
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? 0 : it->second.TotalWeight;
    }

private:
    i64 TotalWeight;
    i64 PendingWeight;
    i64 CompletedWeight;

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
        PendingWeight -= stripe->Weight;
    }

    template <class TIterator>
    void AddStripes(
        TPoolExtractionResultPtr result,
        const TIterator& begin,
        const TIterator& end,
        i64 weightThreshold,
        const Stroka& address)
    {
        for (auto it = begin; it != end; ++it) {
            if (result->TotalChunkWeight >= weightThreshold) {
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
    : public IChunkPool
{
public:
    TAtomicChunkPool()
        : TotalWeight(0)
        , Extracted(false)
        , Initialized(false)
        , Completed_(false)
    { }

    void Add(TChunkStripePtr stripe)
    {
        YASSERT(!Initialized);
        TotalWeight += stripe->Weight;
        Stripes.push_back(stripe);
        FOREACH (const auto& chunk, stripe->Chunks) {
            const auto& inputChunk = chunk.InputChunk;
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                AddressToLocality[address] += chunk.Weight;
            }
        }
    }

    TPoolExtractionResultPtr Extract(const Stroka& address)
    {
        Initialized = true;
        YASSERT(!Extracted);

        auto result = New<TPoolExtractionResult>();
        FOREACH (const auto& stripe, Stripes) {
            result->Add(stripe, address);
        }

        Extracted = true;
        return result;
    }

    void OnFailed(TPoolExtractionResultPtr result)
    {
        YASSERT(Initialized);
        YASSERT(Extracted);
        Extracted = false;
    }

    void OnCompleted(TPoolExtractionResultPtr result)
    {
        YASSERT(Initialized);
        YASSERT(Extracted);
        Completed_ = true;
    }

    i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    i64 GetPendingWeight() const
    {
        return Extracted ? 0 : TotalWeight;
    }

    i64 GetCompletedWeight() const
    {
        return Completed_ ? TotalWeight : 0;
    }

    bool IsCompleted() const
    {
        return Completed_;
    }

    bool IsPending() const
    {
        return !Extracted && !Stripes.empty();
    }

    i64 GetLocality(const Stroka& address) const
    {
        if (Extracted) {
            return 0;
        }
        auto it = AddressToLocality.find(address);
        return it == AddressToLocality.end() ? 0 : it->second;
    }

private:
    i64 TotalWeight;
    std::vector<TChunkStripePtr> Stripes;
    //! Addresses of added chunks.
    yhash_map<Stroka, i64> AddressToLocality;
    //! Have the stripes been #Extract'ed?
    bool Extracted;
    //! Has any #Extract call been made already?
    bool Initialized;
    //! Were extracted chunks processed successfully?
    bool Completed_;

};

////////////////////////////////////////////////////////////////////

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

