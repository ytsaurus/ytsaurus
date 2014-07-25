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

    explicit TChunkedMemoryPool(
        size_t chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio,
        void* tagCookie = GetRefCountedTrackerCookie<TDefaultChunkedMemoryPoolTag>());

    template <class TTag>
    explicit TChunkedMemoryPool(
        TTag tag = TTag(),
        size_t chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio)
        : TChunkedMemoryPool(
            chunkSize,
            maxSmallBlockSizeRatio,
            GetRefCountedTrackerCookie<TTag>())
    { }

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

    int CurrentChunkIndex_ = 0;

    i64 Size_ = 0;
    i64 Capacity_ = 0;

    char* CurrentPtr_;
    char* EndPtr_;

    std::vector<TSharedRef> Chunks_;
    std::vector<TSharedRef> LargeBlocks_;


    char* AllocateUnalignedSlow(size_t size);

    void AllocateChunk();
    void SwitchChunk();
    void SetupPointers();

    TSharedRef AllocateLargeBlock(size_t size);

};

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to inl
inline char* TChunkedMemoryPool::AllocateUnaligned(size_t size)
{
    // Fast path.
    if (CurrentPtr_ + size <= EndPtr_) {
        char* result = CurrentPtr_;
        CurrentPtr_ += size;
        Size_ += size;
        return result;
    }

    // Slow path.
    return AllocateUnalignedSlow(size);
}

inline char* TChunkedMemoryPool::Allocate(size_t size)
{
    CurrentPtr_ = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(CurrentPtr_) + 7) & ~7);
    return AllocateUnaligned(size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
