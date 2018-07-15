#pragma once

#include "public.h"
#include "ref.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TDefaultChunkedMemoryPoolTag { };

class TAllocationHolder
     : public TMutableRef
{
public:
    TAllocationHolder() = default;
    TAllocationHolder(const TAllocationHolder&) = delete;
    TAllocationHolder(TAllocationHolder&&) = default;

    void operator delete(void* ptr) noexcept
    {
        ::free(ptr);
    }

    template <class TDerived>
    static TDerived* Allocate(size_t size)
    {
        auto totalSize = sizeof(TDerived) + size;
        auto* ptr = ::malloc(totalSize);
        auto* instance = static_cast<TDerived*>(ptr);

        try {
            new (instance) TDerived();
            *static_cast<TMutableRef*>(instance) = TMutableRef(instance + 1, size);
        } catch (const std::exception& ex) {
            // Do not forget to free the memory.
            ::free(ptr);
            throw;
        }

        return instance;
    }

    void SetCookie(TRefCountedTypeCookie cookie)
    {
    #ifdef YT_ENABLE_REF_COUNTED_TRACKING
        if (Cookie_ != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
            TRefCountedTrackerFacade::FreeSpace(Cookie_, Size());
        }
        Cookie_ = cookie;
        if (Cookie_ != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::AllocateTagInstance(Cookie_);
            TRefCountedTrackerFacade::AllocateSpace(Cookie_, Size());
        }
    #endif
    }

    ~TAllocationHolder()
    {
    #ifdef YT_ENABLE_REF_COUNTED_TRACKING
        if (Cookie_ != NullRefCountedTypeCookie) {
            TRefCountedTrackerFacade::FreeTagInstance(Cookie_);
            TRefCountedTrackerFacade::FreeSpace(Cookie_, Size());
        }
    #endif
    }

private:
#ifdef YT_ENABLE_REF_COUNTED_TRACKING
    TRefCountedTypeCookie Cookie_ = NullRefCountedTypeCookie;
#endif
};

struct IMemoryChunkProvider
    : public TIntrinsicRefCounted
{
    virtual std::shared_ptr<TMutableRef> Allocate(TRefCountedTypeCookie cookie) = 0;

    virtual size_t GetChunkSize() const = 0;
};

DEFINE_REFCOUNTED_TYPE(IMemoryChunkProvider)

IMemoryChunkProviderPtr CreateMemoryChunkProvider(i64 chunkSize);

class TChunkedMemoryPool
    : private TNonCopyable
{
public:
    static const i64 DefaultChunkSize;
    static const double DefaultMaxSmallBlockSizeRatio;

    TChunkedMemoryPool(
        double maxSmallBlockSizeRatio,
        TRefCountedTypeCookie tagCookie,
        IMemoryChunkProviderPtr chunkProvider);

    explicit TChunkedMemoryPool(
        i64 chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio,
        TRefCountedTypeCookie tagCookie = GetRefCountedTypeCookie<TDefaultChunkedMemoryPoolTag>())
        : TChunkedMemoryPool(
            maxSmallBlockSizeRatio,
            tagCookie,
            CreateMemoryChunkProvider(chunkSize))
    { }

    template <class TTag>
    explicit TChunkedMemoryPool(
        TTag,
        i64 chunkSize = DefaultChunkSize,
        double maxSmallBlockSizeRatio = DefaultMaxSmallBlockSizeRatio)
        : TChunkedMemoryPool(
            chunkSize,
            maxSmallBlockSizeRatio,
            GetRefCountedTypeCookie<TTag>())
    { }

    template <class TTag>
    explicit TChunkedMemoryPool(
        TTag,
        IMemoryChunkProviderPtr chunkProvider)
        : TChunkedMemoryPool(
            1.0,
            GetRefCountedTypeCookie<TTag>(),
            std::move(chunkProvider))
    { }

    //! Allocates #sizes bytes without any alignment.
    char* AllocateUnaligned(i64 size);

    //! Allocates #size bytes aligned with 8-byte granularity.
    char* AllocateAligned(i64 size, int align = 8);

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
    i64 GetSize() const;

    //! Returns the number of reserved bytes.
    i64 GetCapacity() const;

private:
    const i64 ChunkSize_;
    const i64 MaxSmallBlockSize_;
    const TRefCountedTypeCookie TagCookie_;
    const IMemoryChunkProviderPtr ChunkProvider_;

    int CurrentChunkIndex_ = 0;

    i64 Size_ = 0;
    i64 Capacity_ = 0;

    // Chunk memory layout:
    //   |AAAA|....|UUUU|
    // Legend:
    //   A aligned allocations
    //   U unaligned allocations
    //   . free zone
    char* FreeZoneBegin_;
    char* FreeZoneEnd_;

    char* FirstChunkBegin_ = nullptr;
    char* FirstChunkEnd_ = nullptr;



    std::vector<std::shared_ptr<TMutableRef>> Chunks_;
    std::vector<TSharedMutableRef> LargeBlocks_;

    char* AllocateUnalignedSlow(i64 size);
    char* AllocateAlignedSlow(i64 size, int align);
    char* AllocateSlowCore(i64 size);

    void SetupFreeZone();

    void ClearSlow();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define CHUNKED_MEMORY_POOL_INL_H_
#include "chunked_memory_pool-inl.h"
#undef CHUNKED_MEMORY_POOL_INL_H_
