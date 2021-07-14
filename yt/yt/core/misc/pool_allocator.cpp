#include "pool_allocator.h"

#include <library/cpp/ytalloc/api/ytalloc.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TPoolAllocator::AllocateChunk()
{
    VERIFY_THREAD_AFFINITY(HomeThread);

    auto alignment = Max(
        alignof(TAllocatedBlockHeader),
        alignof(TFreeBlockHeader),
        BlockAlignment_);
    auto alignedHeaderSize = Max(
        AlignUp(sizeof(TAllocatedBlockHeader), alignment),
        AlignUp(sizeof(TFreeBlockHeader), alignment));
    auto alignedBlockSize = AlignUp(BlockSize_, alignment);
    auto fullBlockSize = alignedHeaderSize + alignedBlockSize;

    auto blocksPerChunk = Max<size_t>(ChunkSize_ / fullBlockSize, 1);
    auto chunkSize = NYTAlloc::GetAllocationSize(blocksPerChunk * fullBlockSize);

    auto chunk = TSharedMutableRef::Allocate(
        chunkSize,
        /*initializeStorage*/false,
        Cookie_);
    Chunks_.push_back(chunk);

    auto* current = chunk.Begin();
    while (true) {
        auto* blockBegin = current + alignedHeaderSize;
        auto* blockEnd = blockBegin + alignedBlockSize;
        if (blockEnd > chunk.End()) {
            break;
        }

        auto* header = reinterpret_cast<TFreeBlockHeader*>(blockBegin) - 1;
        new(header) TFreeBlockHeader(FirstFree_);
        FirstFree_ = header;

        current += fullBlockSize;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
