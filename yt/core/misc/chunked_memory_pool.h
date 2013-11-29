#pragma once

#include "common.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TChunkedMemoryPool
{
public:
    explicit TChunkedMemoryPool(
        size_t chunkSize = 64 * 1024, 
        double maxSmallBlockSizeRatio = 0.25);

    //! Allocates #sizes bytes without any alignment.
    char* AllocateUnaligned(size_t size);

    //! Allocates #sizes bytes aligned with 8-byte granularity.
    char* Allocate(size_t size);

    //! Allocates and default-constructs an instance of |T|.
    template <class T>
    T* Allocate()
    {
        char* buffer = Allocate(sizeof (T));
        new (buffer) T();
        return reinterpret_cast<T*>(buffer);
    }

    //! Marks all previously allocated chunks as free for subsequent allocations.
    //! Does not deallocate them.
    void Clear();

private:
    const size_t ChunkSize;
    const size_t MaxSmallBlockSize;

    int ChunkIndex;
    size_t Offset;

    std::vector<TSharedRef> Chunks;
    std::vector<TSharedRef> LargeBlocks;

    void AllocateNewChunk();
    TSharedRef AllocateBlock(size_t size);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
