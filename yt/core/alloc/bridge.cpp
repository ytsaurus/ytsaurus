#include "core.cpp"

////////////////////////////////////////////////////////////////////////////////
// Malloc bridge

using namespace NYT::NYTAlloc;

void* operator new(size_t size)
{
    return YTAlloc(size);
}

void* operator new(size_t size, const std::nothrow_t&) noexcept
{
    return YTAlloc(size);
}

void operator delete(void* ptr) noexcept
{
    YTFree(ptr);
}

void operator delete(void* ptr, const std::nothrow_t&) noexcept
{
    YTFree(ptr);
}

void* operator new[](size_t size)
{
    return YTAlloc(size);
}

void* operator new[](size_t size, const std::nothrow_t&) noexcept
{
    return YTAlloc(size);
}

void operator delete[](void* ptr) noexcept
{
    YTFree(ptr);
}

void operator delete[](void* ptr, const std::nothrow_t&) noexcept
{
    YTFree(ptr);
}

extern "C" void* malloc(size_t size)
{
    return YTAlloc(size);
}

extern "C" void* valloc(size_t size)
{
    return YTAllocPageAligned(size);
}

extern "C" void* aligned_alloc(size_t alignment, size_t size)
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

extern "C" void* pvalloc(size_t size)
{
    return valloc(AlignUp(size, PageSize));
}

extern "C" int posix_memalign(void** ptrPtr, size_t alignment, size_t size)
{
    *ptrPtr = aligned_alloc(alignment, size);
    return 0;
}

extern "C" void* memalign(size_t alignment, size_t size)
{
    return aligned_alloc(alignment, size);
}

extern "C" void* __libc_memalign(size_t alignment, size_t size)
{
    return aligned_alloc(alignment, size);
}

extern "C" void free(void* ptr)
{
    YTFree(ptr);
}

extern "C" void* calloc(size_t n, size_t elemSize)
{
    // Overflow check.
    auto size = n * elemSize;
    if (elemSize != 0 && size / elemSize != n) {
        return nullptr;
    }

    void* result = YTAlloc(size);
    ::memset(result, 0, size);
    return result;
}

extern "C" void cfree(void* ptr)
{
    YTFree(ptr);
}

extern "C" void* realloc(void* oldPtr, size_t newSize)
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

////////////////////////////////////////////////////////////////////////////////
