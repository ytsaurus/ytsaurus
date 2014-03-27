#pragma once

#include "public.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultChunkedMemoryPoolTag { };

class TChunkedMemoryPool
    : private TNonCopyable
{
public:
    static const size_t DefaultChunkSize;
    static const double DefaultMaxSmallBlockSizeRatio;

    inline explicit TChunkedMemoryPool(
        void* tagCookie = GetRefCountedTrackerCookie<TDefaultChunkedMemoryPoolTag>(),
        size_t chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio)
    {
        Initialize(
            chunkSize,
            maxSmallBlockSizeRatio,
            tagCookie);
    }

    template <class TTag>
    explicit TChunkedMemoryPool(
        size_t chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio)
    {
        Initialize(
            GetRefCountedTrackerCookie<TTag>(),
            chunkSize,
            maxSmallBlockSizeRatio)
    }

    //! Allocates #sizes bytes without any alignment.
    char* AllocateUnaligned(size_t size);

    //! Allocates #sizes bytes aligned with 8-byte granularity.
    char* Allocate(size_t size);

    //! Marks all previously allocated small chunks as free for subsequent allocations but
    //! does not deallocate them.
    //! Disposes all large blocks.
    void Clear();

    //! Returns the number of allocated bytes.
    i64 GetSize() const;

    //! Returns the number of reserved bytes.
    i64 GetCapacity() const;

private:
    size_t ChunkSize_;
    size_t MaxSmallBlockSize_;
    void* TagCookie_;

    int CurrentChunkIndex_;
    size_t CurrentOffset_;

    i64 Size_;
    i64 Capacity_;

    std::vector<TSharedRef> Chunks_;
    std::vector<TSharedRef> LargeBlocks_;


    void Initialize(
        size_t chunkSize, 
        double maxSmallBlockSizeRatio,
        void* tagCookie);

    void AllocateChunk();
    TSharedRef AllocateLargeBlock(size_t size);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
