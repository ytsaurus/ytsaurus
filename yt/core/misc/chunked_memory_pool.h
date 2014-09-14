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
        TRefCountedTypeCookie tagCookie = GetRefCountedTypeCookie<TDefaultChunkedMemoryPoolTag>());

    template <class TTag>
    explicit TChunkedMemoryPool(
        TTag tag = TTag(),
        size_t chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio)
        : TChunkedMemoryPool(
            chunkSize,
            maxSmallBlockSizeRatio,
        GetRefCountedTypeCookie<TTag>())
    { }

    //! Allocates #sizes bytes without any alignment.
    char* AllocateUnaligned(size_t size);

    //! Allocates #size bytes aligned with #align-byte granularity.
    /*!
     *  #align must be a power of two.
     */
    char* AllocateAligned(size_t size, size_t align = 8);

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
    TRefCountedTypeCookie TagCookie_;

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

inline char* TChunkedMemoryPool::AllocateAligned(size_t size, size_t align)
{
    CurrentPtr_ = reinterpret_cast<char*>((reinterpret_cast<uintptr_t>(CurrentPtr_) + align - 1) & ~(align - 1));
    return AllocateUnaligned(size);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
