#pragma once

#include "public.h"
#include "ref.h"

namespace NYT {
namespace NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

// Support build without YTAlloc
Y_WEAK size_t YTGetSize(void* ptr)
{
    return 0;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc

////////////////////////////////////////////////////////////////////////////////

struct TDefaultChunkedMemoryPoolTag { };

// TAllocationHolder is polymorphic. So we cannot use TWithExtraSpace mixin
// because it needs the most derived type as a template argument and
// it would require GetExtraSpacePtr/GetRef methods to be virtual.

class TAllocationHolder
{
public:
    TAllocationHolder(TMutableRef ref, TRefCountedTypeCookie cookie);
    TAllocationHolder(const TAllocationHolder&) = delete;
    TAllocationHolder(TAllocationHolder&&) = default;
    virtual ~TAllocationHolder();

    void operator delete(void* ptr) noexcept
    {
        ::free(ptr);
    }

    TMutableRef GetRef() const
    {
        return Ref_;
    }

    template <class TDerived>
    static TDerived* Allocate(size_t size, TRefCountedTypeCookie cookie)
    {
        auto requestedSize = sizeof(TDerived) + size;
        auto* ptr = ::malloc(requestedSize);
        auto allocatedSize = NYTAlloc::YTGetSize(ptr);
        if (allocatedSize) {
            size += allocatedSize - requestedSize;
        }

        auto* instance = static_cast<TDerived*>(ptr);

        try {
            new (instance) TDerived(TMutableRef(instance + 1, size), cookie);
        } catch (const std::exception& ex) {
            // Do not forget to free the memory.
            ::free(ptr);
            throw;
        }

        return instance;
    }

private:
    const TMutableRef Ref_;

#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie Cookie_ = NullRefCountedTypeCookie;
#endif
};

struct IMemoryChunkProvider
    : public TIntrinsicRefCounted
{
    virtual std::unique_ptr<TAllocationHolder> Allocate(size_t size, TRefCountedTypeCookie cookie) = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryChunkProvider)

IMemoryChunkProviderPtr CreateMemoryChunkProvider();

class TChunkedMemoryPool
    : private TNonCopyable
{
public:
    static const size_t DefaultStartChunkSize;
    static const size_t RegularChunkSize;

    TChunkedMemoryPool(
        TRefCountedTypeCookie tagCookie,
        IMemoryChunkProviderPtr chunkProvider,
        size_t startChunkSize = DefaultStartChunkSize);

    TChunkedMemoryPool()
        : TChunkedMemoryPool(
            GetRefCountedTypeCookie<TDefaultChunkedMemoryPoolTag>(),
            CreateMemoryChunkProvider())
    { }

    template <class TTag>
    explicit TChunkedMemoryPool(
        TTag,
        size_t startChunkSize = DefaultStartChunkSize)
        : TChunkedMemoryPool(
            GetRefCountedTypeCookie<TTag>(),
            CreateMemoryChunkProvider(),
            startChunkSize)
    { }

    //! Allocates #sizes bytes without any alignment.
    char* AllocateUnaligned(size_t size);

    //! Allocates #size bytes aligned with 8-byte granularity.
    char* AllocateAligned(size_t size, int align = 8);

    //! Allocates #n uninitialized instances of #T.
    template <class T>
    T* AllocateUninitialized(int n, int align = alignof(T));

    //! Allocates space and copies #src inside it.
    char* Capture(TRef src, int align = 8);

    //! Frees memory range if possible: namely, if the free region is a suffix of last allocated region.
    void Free(char* from, char* to);

    //! Marks all previously allocated small chunks as free for subsequent allocations but
    //! does not deallocate them.
    //! Purges all large blocks.
    void Clear();

    //! Purges all allocated memory, including small chunks.
    void Purge();

    //! Returns the number of allocated bytes.
    size_t GetSize() const;

    //! Returns the number of reserved bytes.
    size_t GetCapacity() const;

private:
    const TRefCountedTypeCookie TagCookie_;
    const IMemoryChunkProviderPtr ChunkProvider_;

    int NextChunkIndex_ = 0;
    size_t NextSmallSize_;

    size_t Size_ = 0;
    size_t Capacity_ = 0;

    // Chunk memory layout:
    //   |AAAA|....|UUUU|
    // Legend:
    //   A aligned allocations
    //   U unaligned allocations
    //   . free zone
    char* FreeZoneBegin_;
    char* FreeZoneEnd_;

    std::vector<std::unique_ptr<TAllocationHolder>> Chunks_;
    std::vector<std::unique_ptr<TAllocationHolder>> OtherBlocks_;

    char* AllocateUnalignedSlow(size_t size);
    char* AllocateAlignedSlow(size_t size, int align);
    char* AllocateSlowCore(size_t size);

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CHUNKED_MEMORY_POOL_INL_H_
#include "chunked_memory_pool-inl.h"
#undef CHUNKED_MEMORY_POOL_INL_H_
