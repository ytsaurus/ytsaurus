#ifndef MEMORY_OPS_INL_H_
#error "Direct inclusion of this file is not allowed, include memory_ops.h"
// For the sake of sane code completion.
#include "memory_ops.h"
#endif

#include <util/system/compiler.h>

#if defined(__SSE2__) && !defined(_asan_enabled_)

#include <emmintrin.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void MemcpySmallUnsafe(
    void * __restrict dst,
    const void* __restrict src,
    ssize_t n)
{
    const auto* typedSrc = reinterpret_cast<const __m128i*>(src);
    auto* typedDst = reinterpret_cast<__m128i*>(dst);
    while (n > 0) {
        _mm_storeu_si128(typedDst++, _mm_loadu_si128(typedSrc++));
        n -= sizeof(__m128i);
    }
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#else

namespace NYT {

/////////////////////////////////////////////////////////////////////////////

Y_FORCE_INLINE void MemcpySmallUnsafe(
    void * __restrict dst,
    const void* __restrict src,
    ssize_t n)
{
    // For unsanitized builds, YTAlloc enables accessing 16 bytes at src.
    // For sanitized builds, we must fall back to memcpy.
    ::memcpy(dst, src, static_cast<size_t>(n));
}

/////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#endif
