#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

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
    TAtomicChunkPool()
        : Extracted(false)
        , Initialized(false)
        , ExtractResult(New<TExtractResult>())
    { }

    virtual void Add(TPooledChunkPtr chunk)
    {
        YASSERT(!Initialized);
        ExtractResult->AddRemote(chunk);
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

        return ExtractResult;
    }

    virtual void PutBack(TExtractResultPtr result)
    {
        YASSERT(result == ExtractResult);
        YASSERT(Initialized);
        YASSERT(Extracted);
        Extracted = false;
    }

private:
    //! Cached extraction result.
    /*!
     *  Pooled chunks are appended here.
     *  In #Extract this result is returned as-is.
     */
    TExtractResultPtr ExtractResult;
    //! Is pending #Extract is progress?
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

