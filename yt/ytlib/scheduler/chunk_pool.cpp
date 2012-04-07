#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////

class TUnorderedChunkPool
    : public IChunkPool
{
public:
    virtual void Add(TPooledChunkPtr chunk)
    {
        YASSERT(chunk->Weight > 0);

        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            YVERIFY(AddressToChunks[address].insert(chunk).second);
        }
        YVERIFY(Chunks.insert(chunk).second);
    }

    virtual TExtractResultPtr Extract(
        const Stroka& address,
        i64 weightThreshold,
        int maxCount,
        bool needLocal)
    {
        auto result = New<TExtractResult>();
        int extractedCount = 0;

        // Take local chunks first.
        auto addressIt = AddressToChunks.find(address);
        if (addressIt != AddressToChunks.end()) {
            const auto& localChunks = addressIt->second;
            FOREACH (const auto& chunk, localChunks) {
                if (extractedCount >= maxCount || result->Weight >= weightThreshold) {
                    break;
                }
                result->AddLocal(chunk);
                ++extractedCount;
            }
        }

        if (result->LocalCount == 0 && needLocal) {
            // Could not find any local chunks but requested so.
            // Don't look at remote ones.
            return NULL;
        }

        // Unregister taken local chunks.
        // We have to do this right away, otherwise we risk getting same chunks
        // in the next phase.
        for (int i = 0; i < result->LocalCount; ++i) {
            Extract(result->Chunks[i]);
        }

        // Take remote chunks.
        FOREACH (const auto& chunk, Chunks) {
            if (extractedCount >= maxCount || result->Weight >= weightThreshold) {
                break;
            }
            result->AddRemote(chunk);
            ++extractedCount;
        }

        // Unregister taken remote chunks.
        for (int i = result->LocalCount; i < result->LocalCount + result->RemoteCount; ++i) {
            Extract(result->Chunks[i]);
        }

        return result;
    }

    void PutBack(TExtractResultPtr result)
    {
        FOREACH (const auto& chunk, result->Chunks) {
            Add(chunk);
        }
    }

private:
    yhash_map<Stroka, yhash_set<TPooledChunkPtr> > AddressToChunks;
    yhash_set<TPooledChunkPtr> Chunks;
    
    void Extract(TPooledChunkPtr chunk)
    {
        FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
            YVERIFY(AddressToChunks[address].erase(chunk) == 1);
        }
        YVERIFY(Chunks.erase(chunk) == 1);
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
    virtual void Add(TPooledChunkPtr chunk)
    {
        Chunks.push_back(chunk);
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
        auto result = New<TExtractResult>();
        FOREACH (const auto& chunk, Chunks) {
            result->AddRemote(chunk);
        }
        return result;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        YASSERT(Chunks.empty());
        FOREACH (const auto& chunk, result->Chunks) {
            Add(chunk);
        }
    }

private:
    std::vector<TPooledChunkPtr> Chunks;

};

TAutoPtr<IChunkPool> CreateAtomicChunkPool()
{
    return new TAtomicChunkPool();
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

