#include "chunked_memory_pool.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TChunkedMemoryPool::TChunkedMemoryPool(
    size_t chunkSize,
    double maxSmallBlockSizeRatio)
    : ChunkSize((chunkSize + 7) & ~7) // must be aligned
    , MaxSmallBlockSize(static_cast<size_t>(ChunkSize * maxSmallBlockSizeRatio))
    , ChunkIndex(0)
    , Offset(0)
{ }

char* TChunkedMemoryPool::AllocateUnaligned(size_t size)
{
    while (true) {
        if (ChunkIndex == Chunks.size()) {
            AllocateNewChunk();
        }

        auto& chunk = Chunks[ChunkIndex];
        if (Offset + size < chunk.Size()) {
            auto* result = chunk.Begin() + Offset;
            Offset += size;
            return result;
        } 

        if (size > MaxSmallBlockSize) {
            auto result = AllocateBlock(size);
            LargeBlocks.push_back(result);
            return result.Begin();
        }

        Offset = 0;
        ++ChunkIndex;
    }
}

char* TChunkedMemoryPool::Allocate(size_t size)
{
    Offset = (Offset + 7) & ~7;
    return AllocateUnaligned(size);
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
    auto block = TSharedRef::Allocate<TChunkedMemoryPoolTag>(size);
    // Ensure proper initial alignment (only makes sense for 32-bit platforms).
    if ((reinterpret_cast<intptr_t>(block.Begin()) & 7) != 0) {
        Offset = 8 - reinterpret_cast<intptr_t>(block.Begin()) & 7;
    }
    return block;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
