#pragma once

#include "public.h"

namespace NYT {
namespace NYTAlloc {

////////////////////////////////////////////////////////////////////////////////

//! Allocates a chunk of memory of (at least) #size bytes.
//! The returned pointer is guaranteed to be 16-byte aligned.
void* YTAlloc(size_t size);

//! Allocates a chunk of memory of (at least) #size bytes.
//! The returned pointer is guaranteed to be 4096-byte aligned.
void* YTAllocPageAligned(size_t size);

//! Frees a chunk of memory previously allocated via YTAlloc* functions.
//! Does nothing if #ptr is null.
void YTFree(void* ptr);

//! Returns the size of the chunk pointed to by #ptr.
//! This size is not guaranteed to be exactly equal to #size passed to YTAlloc* functions
//! due to rounding; the returned size, however, is never less than the latter size.
//! If #ptr is null then returns 0.
size_t YTGetSize(void* ptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc
} // namespace NYT
