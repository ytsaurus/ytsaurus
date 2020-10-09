#pragma once

#include <sys/types.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MemcpySmallUnsafePadding = 15;

//! |memcpy| optimized for small sizes.
//! Assumes that at least #MemcpySmallUnsafePadding bytes of #src are readable past |src + n|;
//! this is always true if #src points to memory chunk allocated via YTAlloc.
//! Assumes that at least #MemcpySmallUnsafePadding bytes of #dst are writeable.
//! it is caller's responsibility to allocate #dst with an appropriate padding.
void MemcpySmallUnsafe(
    void * __restrict dst,
    const void* __restrict src,
    ssize_t n);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MEMORY_OPS_INL_H_
#include "memory_ops-inl.h"
#undef MEMORY_OPS_INL_H_
