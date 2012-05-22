#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

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
    FOREACH (const auto& chunk, stripe->InputChunks) {
        ++TotalChunkCount;
        if (std::find_if(
            chunk.node_addresses().begin(),
            chunk.node_addresses().end(),
            [&] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != chunk.node_addresses().end())
        {
            ++LocalChunkCount;
        } else {
            ++RemoteChunkCount;
        }
    }
}

////////////////////////////////////////////////////////////////////

class TChunkPool::TImpl
{
public:
    TImpl()
        : TotalWeight(0)
        , PendingWeight(0)
    { }

    void Add(TChunkStripePtr stripe)
    {
        YASSERT(stripe->Weight > 0);
        TotalWeight += stripe->Weight;
        PendingWeight += stripe->Weight;
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                YVERIFY(LocalChunks[address].insert(stripe).second);
            }
        }
        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    TPoolExtractionResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal)
    {
        auto result = New<TPoolExtractionResult>();

        // Take local chunks first.
        auto addressIt = LocalChunks.find(address);
        if (addressIt != LocalChunks.end()) {
            const auto& localChunks = addressIt->second;
            AddStripes(
                result,
                localChunks.begin(),
                localChunks.end(),
                weightThreshold,
                address);
        }

        if (result->LocalChunkCount == 0 && needLocal) {
            // Could not find any local chunks but requested to do so.
            // Don't look at remote ones.
            return NULL;
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

    void PutBack(TPoolExtractionResultPtr result)
    {
        FOREACH (const auto& stripe, result->Stripes) {
            Add(stripe);
        }
    }

    i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    i64 GetPendingWeight() const
    {
        return PendingWeight;
    }

    bool HasPendingChunks() const
    {
        return !GlobalChunks.empty();
    }

    bool HasPendingLocalChunksFor(const Stroka& address) const
    {
        auto it = LocalChunks.find(address);
        return it == LocalChunks.end() ? false : !it->second.empty();
    }

private:
    i64 TotalWeight;
    i64 PendingWeight;
    yhash_map<Stroka, yhash_set<TChunkStripePtr> > LocalChunks;
    yhash_set<TChunkStripePtr> GlobalChunks;
    
    void Unregister(TChunkStripePtr stripe)
    {
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.node_addresses()) {
                YVERIFY(LocalChunks[address].erase(stripe) == 1);
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

////////////////////////////////////////////////////////////////////

TChunkPool::TChunkPool()
    : Impl(new TImpl())
{ }

TChunkPool::~TChunkPool()
{ }

void TChunkPool::Add(TChunkStripePtr stripe)
{
    Impl->Add(stripe);
}

TPoolExtractionResultPtr TChunkPool::Extract(
    const Stroka& address,
    i64 weightThreshold,
    bool needLocal)
{
    return Impl->Extract(
        address,
        weightThreshold,
        needLocal);
}

void TChunkPool::PutBack(TPoolExtractionResultPtr result)
{
    Impl->PutBack(result);
}

i64 TChunkPool::GetTotalWeight() const
{
    return Impl->GetTotalWeight();
}

i64 TChunkPool::GetPendingWeight() const
{
    return Impl->GetPendingWeight();
}

bool TChunkPool::HasPendingChunks() const
{
    return Impl->HasPendingChunks();
}

bool TChunkPool::HasPendingLocalChunksFor(const Stroka& address) const
{
    return Impl->HasPendingLocalChunksFor(address);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

