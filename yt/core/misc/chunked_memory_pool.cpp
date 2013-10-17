#include "chunked_memory_pool.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool::TChunkedMemoryPool(size_t chunkSize, size_t maxSmallBlockSize)
    : ChunkSize(chunkSize)
    , MaxSmallBlockSize(maxSmallBlockSize)
    , ChunkIndex(0)
    , Offset(0)
{ 
    AllocateNewChunk();
}

void* TChunkedMemoryPool::AllocateAligned(size_t size)
{
    // Round to nearest multiplier of 8
    Offset = (((Offset - 1) >> 3) + 1) << 3;

    auto& currentChunk = Chunks[ChunkIndex];
    if (Offset + size < currentChunk.Size()) {
        auto result = currentChunk.Begin() + Offset;
        Offset += size;
        return result;
    } 

    if (size > MaxSmallBlockSize) {
        auto res = AllocateBlock(size);
        LargeBlocks.push_back(res);
        return res.Begin();
    }

    Offset = 0;
    ++ChunkIndex;

    if (ChunkIndex == Chunks.size()) {
        AllocateNewChunk();
    }

    return Chunks.back().Begin();
}

void TChunkedMemoryPool::Clear()
{
    Offset = 0;
    ChunkIndex = 0;
    LargeBlocks.clear();
}

void TChunkedMemoryPool::AllocateNewChunk()
{
    Chunks.push_back(AllocateBlock(ChunkSize));
}

TSharedRef TChunkedMemoryPool::AllocateBlock(size_t size)
{
    struct TChunkedMemoryPoolTag { };
    return TSharedRef::Allocate<TChunkedMemoryPoolTag>(size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
