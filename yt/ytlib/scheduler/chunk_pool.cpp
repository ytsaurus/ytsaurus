#include "stdafx.h"
#include "chunk_pool.h"
#include "private.h"

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////

void TUnorderedChunkPool::Put(TPooledChunkPtr chunk)
{
    YASSERT(chunk->Weight > 0);

    FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
        YVERIFY(AddressToChunks[address].insert(chunk).second);
    }
    YVERIFY(Chunks.insert(chunk).second);
}

void TUnorderedChunkPool::Put(const std::vector<TPooledChunkPtr>& chunks)
{
    FOREACH (const auto& chunk, chunks) {
        Put(chunk);
    }
}

void TUnorderedChunkPool::Extract(
    const Stroka& address,
    i64 weightThreshold,
    bool needLocal,
    std::vector<TPooledChunkPtr>* extractedChunks,
    i64* extractedWeight,
    int* localCount,
    int* remoteCount)
{
    *extractedWeight = 0;
    *localCount = 0;
    *remoteCount = 0;

    // Just to be sure.
    extractedChunks->clear();

    // Take local chunks first.
    auto addressIt = AddressToChunks.find(address);
    if (addressIt != AddressToChunks.end()) {
        const auto& localChunks = addressIt->second;
        FOREACH (const auto& chunk, localChunks) {
            if (*extractedWeight >= weightThreshold) {
                break;
            }
            extractedChunks->push_back(chunk);
            ++*localCount;
            *extractedWeight += chunk->Weight;
        }
    }

    if (*localCount == 0 && needLocal) {
        // Could not find any local chunks but requested so.
        // Don't look at remote ones.
        return;
    }

    // Unregister taken local chunks.
    // We have to do this right away, otherwise we risk getting same chunks
    // in the next phase.
    for (int i = 0; i < *localCount; ++i) {
        Extract((*extractedChunks)[i]);
    }

    // Take remote chunks.
    FOREACH (const auto& chunk, Chunks) {
        if (*extractedWeight >= weightThreshold) {
            break;
        }
        extractedChunks->push_back(chunk);
        ++*remoteCount;
        *extractedWeight += chunk->Weight;
    }

    // Unregister taken remote chunks.
    for (int i = *localCount; i < *localCount + *remoteCount; ++i) {
        Extract((*extractedChunks)[i]);
    }
}

void TUnorderedChunkPool::Extract(TPooledChunkPtr chunk)
{
    FOREACH (const auto& address, chunk->InputChunk.holder_addresses()) {
        YVERIFY(AddressToChunks[address].erase(chunk) == 1);
    }
    YVERIFY(Chunks.erase(chunk) == 1);
}

////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT

