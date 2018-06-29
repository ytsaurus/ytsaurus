#pragma once

#include "public.h"

#include <yt/core/misc/enum.h>

namespace NYT {
namespace NYTAlloc {

////////////////////////////////////////////////////////////////////////////////
// Allocation API

//! Allocates a chunk of memory of (at least) #size bytes.
//! The returned pointer is guaranteed to be 16-byte aligned.
void* YTAlloc(size_t size);

//! Allocates a chunk of memory of (at least) #size bytes.
//! The returned pointer is guaranteed to be 4K-byte aligned.
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
// Configuration API

// For large blobs, YTAlloc keeps at least
// max(LargeUnreclaimableBytes, LargeUnreclaimableCoeff * TotalLargeBytesUsed)
// bytes of pooled (unreclaimable) memory.
void SetLargeUnreclaimableCoeff(double value);
void SetLargeUnreclaimableBytes(size_t value);

////////////////////////////////////////////////////////////////////////////////
// Statistics API

DEFINE_ENUM(EBasicCounter,
    (BytesAllocated)
    (BytesFreed)
    (BytesUsed)
);

using ETotalCounter = EBasicCounter;
using ESystemCounter = EBasicCounter;

DEFINE_ENUM(ESmallArenaCounter,
    (PagesMapped)
    (BytesMapped)
    (PagesActive)
    (BytesActive)
);

DEFINE_ENUM(ELargeArenaCounter,
    (BytesSpare)
    (BytesOverhead)
    (BlobsAllocated)
    (BlobsFreed)
    (BlobsUsed)
    (BytesAllocated)
    (BytesFreed)
    (BytesUsed)
    (ExtentsAllocated)
    (PagesMapped)
    (BytesMapped)
    (PagesPopulated)
    (BytesPopulated)
    (PagesReleased)
    (BytesReleased)
    (PagesActive)
    (BytesActive)
    (OverheadBytesReclaimed)
    (SpareBytesReclaimed)
);

DEFINE_ENUM(EHugeArenaCounter,
    (BytesAllocated)
    (BytesFreed)
    (BytesUsed)
    (BlobsAllocated)
    (BlobsFreed)
    (BlobsUsed)
);

constexpr size_t SmallRankCount = 25;
constexpr size_t LargeRankCount = 30;

// Returns statistics for all user allocations.
TEnumIndexedVector<ssize_t, ETotalCounter> GetTotalCounters();

// Returns per-arena statistics for small allocations; these are included into total statistics.
std::array<TEnumIndexedVector<ssize_t, ESmallArenaCounter>, SmallRankCount> GetSmallArenaCounters();

// Returns per-arena statistics for large allocations; these are included into total statistics.
std::array<TEnumIndexedVector<ssize_t, ELargeArenaCounter>, LargeRankCount> GetLargeArenaCounters();

// Returns statistics for huge allocations; these are included into total statistics.
TEnumIndexedVector<ssize_t, EHugeArenaCounter> GetHugeArenaCounters();

// Returns statistics for all system allocations; these are not included into total statistics.
TEnumIndexedVector<ssize_t, ESystemCounter> GetSystemCounters();

// Builds a string containing some brief allocation statistics.
TString FormatCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc
} // namespace NYT
