#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

namespace {

bool IsChunkLocal(TPooledChunkPtr chunk, const Stroka& address)
{
    FOREACH (const auto& chunkAddress, chunk->InputChunk.holder_addresses()) {
        if (chunkAddress == address) {
            return true;
        }
    }
    return false;
}

template <class TIterator>
void AddChunks(
    IChunkPool::TExtractResultPtr result,
    const TIterator& begin,
    const TIterator& end,
    i64 weightThreshold,
    int maxCount,
    bool local)
{
    for (auto it = begin; it != end; ++it) {
        if (result->Chunks.size() >= maxCount || result->Weight >= weightThreshold) {
            break;
        }
        result->Add(*it, local);
    }
}

} // namespace

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public IChunkPool
{
public:
    TUnorderedChunkPool()
        : TotalWeight(0)
    { }

    virtual void Add(TPooledChunkPtr chunk)
    {
        YASSERT(chunk->Weight > 0);
        TotalWeight += chunk->Weight;
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            YVERIFY(LocalChunks[address].insert(chunk).second);
        }
        YVERIFY(GlobalChunks.insert(chunk).second);
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        auto result = New<TExtractResult>();

        // Take local chunks first.
        auto addressIt = LocalChunks.find(address);
        if (addressIt != LocalChunks.end()) {
            const auto& localChunks = addressIt->second;
            AddChunks(
                result,
                localChunks.begin(),
                localChunks.end(),
                weightThreshold,
                maxCount,
                true);
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
            Unregister(result->Chunks[index]);
        }

        // Take remote chunks.
        AddChunks(
            result,
            GlobalChunks.begin(),
            GlobalChunks.end(),
            weightThreshold,
            maxCount,
            false);

        // Unregister taken remote chunks.
        for (int index = result->LocalCount; index < result->LocalCount + result->RemoteCount; ++index) {
            Unregister(result->Chunks[index]);
        }

        return result;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        FOREACH (const auto& chunk, result->Chunks) {
            Add(chunk);
        }
    }

    virtual i64 GetTotalWeight() const
    {
        return TotalWeight;
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
    yhash_map<Stroka, yhash_set<TPooledChunkPtr> > LocalChunks;
    yhash_set<TPooledChunkPtr> GlobalChunks;
    
    void Unregister(TPooledChunkPtr chunk)
    {
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            YVERIFY(LocalChunks[address].erase(chunk) == 1);
        }
        YVERIFY(GlobalChunks.erase(chunk) == 1);
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

    virtual void Add(TPooledChunkPtr chunk)
    {
        YASSERT(!Initialized);
        TotalWeight += chunk->Weight;
        Chunks.push_back(chunk);
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            Addresses.insert(address);
        }
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        UNUSED(address);
        UNUSED(weightThreshold);
        UNUSED(maxCount);
        UNUSED(needLocal);
        
        Initialized = true;
        YASSERT(!Extracted);

        auto result = New<TExtractResult>();
        FOREACH (const auto& chunk, Chunks) {
            result->Add(chunk, IsChunkLocal(chunk, address));
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

    virtual bool HasPendingChunks() const
    {
        return !Extracted && !Chunks.empty();
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
    std::vector<TPooledChunkPtr> Chunks;
    //! Addresses of added chunks.
    yhash_set<Stroka> Addresses;
    //! Have the chunks been #Extract'ed?
    bool Extracted;
    //! Has any #Extract call been made already?
    bool Initialized;
};

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

class TMergeChunkPool
    : public IChunkPool
{
public:
    TMergeChunkPool()
        : TotalWeight(0)
    { }

    virtual void Add(TPooledChunkPtr chunk)
    {
        YASSERT(chunk->Weight > 0);
        TotalWeight += chunk->Weight;
        TChunkIterators its;
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            its.LocalIterators.push_back(LocalChunks[address].insert(chunk));
        }
        its.GlobalIterator = GlobalChunks.insert(chunk);
        YVERIFY(Iterators.insert(MakePair(chunk, its)).second);
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        auto result = New<TExtractResult>();

        // Take local chunks first.
        auto addressIt = LocalChunks.find(address);
        if (addressIt != LocalChunks.end()) {
            const auto& localChunks = addressIt->second;
            AddChunks(
                result,
                localChunks.begin(),
                localChunks.end(),
                weightThreshold,
                maxCount,
                true);
        }

        if (result->LocalCount == 0 && needLocal) {
            // Could not find any local chunks but requested so.
            // Don't look at remote ones.
            return NULL;
        }

        // Unregister taken local chunks.
        // We have to do this right away, otherwise we risk getting same chunks
        // in the next phase.
        for (int index = 0; index < result->LocalCount; ++index) {
            Unregister(result->Chunks[index]);
        }

        // Take remote chunks.

        // Compute pivot based on the last picked chunk.
        auto pivotIt =
            result->Chunks.empty()
            ? GlobalChunks.begin()
            : GlobalChunks.upper_bound(result->Chunks.back());

        // Take remote chunks larger than pivot.
        AddChunks(
            result,
            pivotIt,
            GlobalChunks.end(),
            weightThreshold,
            maxCount,
            false);

        // Take remote chunks smaller than pivot.
        AddChunks(
            result,
            std::reverse_iterator<TOrderedChunkSet::iterator>(pivotIt),
            std::reverse_iterator<TOrderedChunkSet::iterator>(GlobalChunks.begin()),
            weightThreshold,
            maxCount,
            false);

        // Unregister taken remote chunks.
        for (int index = result->LocalCount; index < result->LocalCount + result->RemoteCount; ++index) {
            Unregister(result->Chunks[index]);
        }

        return result;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        FOREACH (const auto& chunk, result->Chunks) {
            Add(chunk);
        }
    }

    virtual i64 GetTotalWeight() const
    {
        return TotalWeight;
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

    struct TChunkWeightComparer
    {
        bool operator ()(TPooledChunkPtr lhs, TPooledChunkPtr rhs) const
        {
            return lhs->Weight < rhs->Weight;
        }
    };

    typedef std::multiset<TPooledChunkPtr, TChunkWeightComparer> TOrderedChunkSet;

    struct TChunkIterators
    {
        TPooledChunkPtr Chunk;
        std::vector<TOrderedChunkSet::iterator> LocalIterators;
        TOrderedChunkSet::iterator GlobalIterator;
    };

    yhash_map<TPooledChunkPtr, TChunkIterators> Iterators;
    yhash_map<Stroka, TOrderedChunkSet> LocalChunks;
    TOrderedChunkSet GlobalChunks;

    void Unregister(TPooledChunkPtr chunk)
    {
        auto& its = Iterators[chunk];
        for (int index = 0; index < static_cast<int>(chunk->InputChunk.holder_addresses_size()); ++index) {
            auto address = chunk->InputChunk.holder_addresses(index);
            auto localIt = its.LocalIterators[index];
            LocalChunks[address].erase(localIt);
        }
        GlobalChunks.erase(its.GlobalIterator);
        YVERIFY(Iterators.erase(chunk) == 1);
    }
};

TAutoPtr<IChunkPool> CreateMergeChunkPool()
{
    return new TMergeChunkPool();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

