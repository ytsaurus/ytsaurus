#include "core.cpp"

#include <util/system/compiler.h>

namespace NYT {
namespace NYTAlloc {

////////////////////////////////////////////////////////////////////////////////
// YTAlloc public API

void* YTAlloc(size_t size)
{
    return InlineYTAlloc(size);
}

void* YTAllocPageAligned(size_t size)
{
    return InlineYTAllocPageAligned(size);
}

void YTFree(void* ptr)
{
    InlineYTFree(ptr);
}

#if !defined(_darwin_) and !defined(_asan_enabled_) and !defined(_msan_enabled_) and !defined(_tsan_enabled_)

size_t YTGetSize(void* ptr)
{
    return InlineYTGetSize(ptr);
}

#endif

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc
} // namespace NYT

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
    return InlineYTAlloc(size);
}

extern "C" YTALLOC_WEAK void* valloc(size_t size)
{
    return YTAllocPageAligned(size);
}

extern "C" YTALLOC_WEAK void* aligned_alloc(size_t alignment, size_t size)
{
    // Alignment must be a power of two.
    YCHECK((alignment & (alignment - 1)) == 0);
    // Alignment must be exceeed page size.
    YCHECK(alignment <= PageSize);
    if (alignment <= 16) {
        // Proper alignment here is automatic.
        return YTAlloc(size);
    } else {
        return YTAllocPageAligned(size);
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
    InlineYTFree(ptr);
}

extern "C" YTALLOC_WEAK void* calloc(size_t n, size_t elemSize)
{
    // Overflow check.
    auto size = n * elemSize;
    if (elemSize != 0 && size / elemSize != n) {
        return nullptr;
    }

    void* result = InlineYTAlloc(size);
    ::memset(result, 0, size);
    return result;
}

extern "C" YTALLOC_WEAK void cfree(void* ptr)
{
    YTFree(ptr);
}

extern "C" YTALLOC_WEAK void* realloc(void* oldPtr, size_t newSize)
{
    if (!oldPtr) {
        return YTAlloc(newSize);
    }

    if (newSize == 0) {
        YTFree(oldPtr);
        return nullptr;
    }

    void* newPtr = YTAlloc(newSize);
    size_t oldSize = YTGetSize(oldPtr);
    ::memcpy(newPtr, oldPtr, std::min(oldSize, newSize));
    YTFree(oldPtr);
    return newPtr;
}

#endif

////////////////////////////////////////////////////////////////////////////////
