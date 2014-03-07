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
    T* Allocate();

    //! Marks all previously allocated small chunks as free for subsequent allocations but
    //! does not deallocate them.
    //! Disposes all large blocks.
    void Clear();

    //! Returns the number of allocated bytes.
    i64 GetSize() const;

    //! Returns the number of reserved bytes.
    i64 GetCapacity() const;

private:
    const size_t ChunkSize_;
    const size_t MaxSmallBlockSize_;

    int CurrentChunkIndex_;
    size_t CurrentOffset_;

    i64 Size_;
    i64 Capacity_;

    std::vector<TSharedRef> Chunks_;
    std::vector<TSharedRef> LargeBlocks_;

    void AllocateChunk();
    TSharedRef AllocateLargeBlock(size_t size);

};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl
template <class T>
T* TChunkedMemoryPool::Allocate()
{
    char* buffer = Allocate(sizeof (T));
    new (buffer)T();
    return reinterpret_cast<T*>(buffer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
