#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

namespace {

template <class TIterator>
void AddStripes(
    IChunkPool::TExtractResultPtr result,
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


} // namespace

////////////////////////////////////////////////////////////////////

IChunkPool::TExtractResult::TExtractResult()
    : TotalChunkWeight(0)
    , TotalChunkCount(0)
    , LocalChunkCount(0)
    , RemoteChunkCount(0)
{ }

void IChunkPool::TExtractResult::Add(TChunkStripePtr stripe, const Stroka& address)
{
    Stripes.push_back(stripe);
    TotalChunkWeight += stripe->Weight;
    FOREACH (const auto& chunk, stripe->InputChunks) {
        ++TotalChunkCount;
        if (std::find_if(
            chunk.holder_addresses().begin(),
            chunk.holder_addresses().end(),
            [&] (const Stroka& chunkAddress) { return address == chunkAddress; })
            != chunk.holder_addresses().end())
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
    { }

    virtual void Add(TChunkStripePtr stripe)
    {
        YASSERT(stripe->Weight > 0);
        TotalWeight += stripe->Weight;
        PendingWeight += stripe->Weight;
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.holder_addresses()) {
                YVERIFY(LocalChunks[address].insert(stripe).second);
            }
        }
        YVERIFY(GlobalChunks.insert(stripe).second);
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal)
    {
        auto result = New<TExtractResult>();

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

        if (result->LocalCount == 0 && needLocal) {
            // Could not find any local chunks but requested to do so.
            // Don't look at remote ones.
            return NULL;
        }

        // Unregister taken local chunks.
        // We have to do this right away, otherwise we risk getting same chunks
        // in the next phase.
        for (int index = 0; index < result->LocalCount; ++index) {
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
        for (int index = result->LocalCount; index < result->LocalCount + result->RemoteCount; ++index) {
            Unregister(result->Stripes[index]);
        }

        return result;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        FOREACH (const auto& stripe, result->Stripes) {
            Add(stripe);
        }
    }

    virtual i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    virtual i64 GetPendingWeight() const
    {
        return PendingWeight;
    }

    virtual bool HasPendingChunks() const
    {
        return !GlobalChunks.empty();
    }

    virtual bool HasPendingLocalChunksFor(const Stroka& address) const
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
            FOREACH (const auto& address, inputChunk.holder_addresses()) {
                YVERIFY(LocalChunks[address].erase(stripe) == 1);
            }
        }
        YVERIFY(GlobalChunks.erase(stripe) == 1);
        PendingWeight -= stripe->Weight;
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
    { }

    virtual void Add(TChunkStripePtr stripe)
    {
        YASSERT(!Initialized);
        TotalWeight += stripe->Weight;
        Stripes.push_back(stripe);
        FOREACH (const auto& inputChunk, stripe->InputChunks) {
            FOREACH (const auto& address, inputChunk.holder_addresses()) {
                Addresses.insert(address);
            }
        }
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        bool needLocal)
    {
        UNUSED(address);
        UNUSED(weightThreshold);
        UNUSED(needLocal);
        
        Initialized = true;
        YASSERT(!Extracted);

        auto result = New<TExtractResult>();
        FOREACH (const auto& stripe, Stripes) {
            result->Add(stripe, address);
        }
        Extracted = true;
        return result;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        YASSERT(Initialized);
        YASSERT(Extracted);
        Extracted = false;
    }

    virtual i64 GetTotalWeight() const
    {
        return TotalWeight;
    }

    virtual i64 GetPendingWeight() const
    {
        return Extracted ? 0 : TotalWeight;
    }

    virtual bool HasPendingChunks() const
    {
        return !Extracted && !Stripes.empty();
    }

    virtual bool HasPendingLocalChunksFor(const Stroka& address) const
    {
        return
            Extracted
            ? false
            : Addresses.find(address) != Addresses.end();
    }

private:
    i64 TotalWeight;
    std::vector<TChunkStripePtr> Stripes;
    //! Addresses of added chunks.
    yhash_set<Stroka> Addresses;
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

