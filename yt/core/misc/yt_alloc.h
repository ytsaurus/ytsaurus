#pragma once

#include <stddef.h>

namespace NYT::NYTAlloc {

////////////////////////////////////////////////////////////////////////////////
// This file can be safely included by anyone interested in basic YTAlloc
// functions (e.g. memory allocation and disposal).
//
// If YTAlloc gets linked in, it provides an efficient implementation.
// Otherwise (non-YTAlloc build), weaks implementations from yt_alloc_fallback.cpp
// are used.
//
// For the explanation on these fuctions, see yt/core/alloc/alloc.h.
////////////////////////////////////////////////////////////////////////////////

constexpr size_t SmallRankCount = 25;
constexpr size_t LargeRankCount = 30;

void* Allocate(size_t size);

template <size_t Size>
void* AllocateConstSize();

void Free(void* ptr);

void FreeNonNull(void* ptr);

size_t GetAllocationSize(void* ptr);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYTAlloc

#define YT_ALLOC_INL_H_
#include "yt_alloc-inl.h"
#undef YT_ALLOC_INL_H_
