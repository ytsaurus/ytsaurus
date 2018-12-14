#include "core.cpp"

#include <util/system/compiler.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////
// YTAlloc public API

#if !defined(_darwin_) and !defined(_asan_enabled_) and !defined(_msan_enabled_) and !defined(_tsan_enabled_)

void* Allocate(size_t size, bool dumpable)
{
    return AllocateInline(size, dumpable);
}

void* AllocatePageAligned(size_t size, bool dumpable)
{
    return AllocatePageAlignedInline(size, dumpable);
}

void Free(void* ptr)
{
    FreeInline(ptr);
}

size_t GetAllocationSize(void* ptr)
{
    return GetAllocationSizeInline(ptr);
}

#else

void* Allocate(size_t size, bool dumpable)
{
    return ::malloc(size);
}

void* AllocatePageAligned(size_t size, bool /*dumpable*/)
{
    return ::valloc(size);
}

void Free(void* ptr)
{
    ::free(ptr);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Memory tags API bridge

TMemoryTag GetCurrentMemoryTag()
{
    return NYTAlloc::TThreadManager::GetCurrentMemoryTag();
}

void SetCurrentMemoryTag(TMemoryTag tag)
{
    NYTAlloc::TThreadManager::SetCurrentMemoryTag(tag);
}

void GetMemoryUsageForTags(TMemoryTag* tags, size_t count, size_t* result)
{
    NYTAlloc::StatisticsManager->GetTaggedMemoryUsage(MakeRange(tags, count), result);
}

size_t GetMemoryUsageForTag(TMemoryTag tag)
{
    size_t result;
    GetMemoryUsageForTags(&tag, 1, &result);
    return result;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

////////////////////////////////////////////////////////////////////////////////
// Malloc bridge

#if !defined(_darwin_) and !defined(_asan_enabled_) and !defined(_msan_enabled_) and !defined(_tsan_enabled_)

using namespace NYT::NYTAlloc;

#define YTALLOC_WEAK __attribute__((weak))

extern "C" YTALLOC_WEAK void* malloc(size_t size)
{
    return AllocateInline(size, true);
}

extern "C" YTALLOC_WEAK void* valloc(size_t size)
{
    return AllocatePageAlignedInline(size, true);
}

extern "C" YTALLOC_WEAK void* aligned_alloc(size_t alignment, size_t size)
{
    // Alignment must be a power of two.
    YCHECK((alignment & (alignment - 1)) == 0);
    // Alignment must be exceeed page size.
    YCHECK(alignment <= PageSize);
    if (alignment <= 16) {
        // Proper alignment here is automatic.
        return Allocate(size, true);
    } else {
        return AllocatePageAligned(size, true);
    }
}

extern "C" YTALLOC_WEAK void* pvalloc(size_t size)
{
    return valloc(AlignUp(size, PageSize));
}

extern "C" YTALLOC_WEAK int posix_memalign(void** ptrPtr, size_t alignment, size_t size)
{
    *ptrPtr = aligned_alloc(alignment, size);
    return 0;
}

extern "C" YTALLOC_WEAK void* memalign(size_t alignment, size_t size)
{
    return aligned_alloc(alignment, size);
}

extern "C" void* __libc_memalign(size_t alignment, size_t size)
{
    return aligned_alloc(alignment, size);
}

extern "C" YTALLOC_WEAK void free(void* ptr)
{
    FreeInline(ptr);
}

extern "C" YTALLOC_WEAK void* calloc(size_t n, size_t elemSize)
{
    // Overflow check.
    auto size = n * elemSize;
    if (elemSize != 0 && size / elemSize != n) {
        return nullptr;
    }

    void* result = Allocate(size);
    ::memset(result, 0, size);
    return result;
}

extern "C" YTALLOC_WEAK void cfree(void* ptr)
{
    Free(ptr);
}

extern "C" YTALLOC_WEAK void* realloc(void* oldPtr, size_t newSize)
{
    if (!oldPtr) {
        return Allocate(newSize);
    }

    if (newSize == 0) {
        Free(oldPtr);
        return nullptr;
    }

    void* newPtr = Allocate(newSize);
    size_t oldSize = GetAllocationSize(oldPtr);
    ::memcpy(newPtr, oldPtr, std::min(oldSize, newSize));
    Free(oldPtr);
    return newPtr;
}

#endif

////////////////////////////////////////////////////////////////////////////////
