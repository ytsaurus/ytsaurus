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

// Calling these functions turns on periodic logging and profiling.
// Once on, these cannot be disabled.
void EnableLogging();
void EnableProfiling();

// For large blobs, YTAlloc keeps at least
// max(LargeUnreclaimableBytes, LargeUnreclaimableCoeff * TotalLargeBytesUsed)
// bytes of pooled (unreclaimable) memory.
void SetLargeUnreclaimableCoeff(double value);
void SetLargeUnreclaimableBytes(size_t value);

// When logging is enabled (see #EnableLogging) and a syscall (mmap, munmap, or madvise)
// takes longer then the configured time, a warning is printed to the log.
void SetSyscallTimeWarningThreshold(TDuration value);
TDuration GetSyscallTimeWarningThreshold();

////////////////////////////////////////////////////////////////////////////////
// Statistics API

DEFINE_ENUM(EBasicCounter,
    (BytesAllocated)
    (BytesFreed)
    (BytesUsed)
);

using ESystemCounter = EBasicCounter;
using ETotalCounter = EBasicCounter;
using ESmallCounter = EBasicCounter;
using ELargeCounter = EBasicCounter;

DEFINE_ENUM(ESmallArenaCounter,
    (PagesMapped)
    (BytesMapped)
    (PagesCommitted)
    (BytesCommitted)
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
    (PagesCommitted)
    (BytesCommitted)
    (OverheadBytesReclaimed)
    (SpareBytesReclaimed)
);

DEFINE_ENUM(EHugeCounter,
    (BytesAllocated)
    (BytesFreed)
    (BytesUsed)
    (BlobsAllocated)
    (BlobsFreed)
    (BlobsUsed)
);

// Returns statistics for all user allocations.
TEnumIndexedVector<ssize_t, ETotalCounter> GetTotalCounters();

// Returns statistics for small allocations; these are included into total statistics.
TEnumIndexedVector<ssize_t, ESmallCounter> GetSmallCounters();

// Returns statistics for large allocations; these are included into total statistics.
TEnumIndexedVector<ssize_t, ELargeCounter> GetLargeCounters();

// Returns per-arena statistics for small allocations; these are included into total statistics.
std::array<TEnumIndexedVector<ssize_t, ESmallArenaCounter>, SmallRankCount> GetSmallArenaCounters();

// Returns per-arena statistics for large allocations; these are included into total statistics.
std::array<TEnumIndexedVector<ssize_t, ELargeArenaCounter>, LargeRankCount> GetLargeArenaCounters();

// Returns statistics for huge allocations; these are included into total statistics.
TEnumIndexedVector<ssize_t, EHugeCounter> GetHugeCounters();

// Returns statistics for all system allocations; these are not included into total statistics.
TEnumIndexedVector<ssize_t, ESystemCounter> GetSystemCounters();

// Builds a string containing some brief allocation statistics.
TString FormatCounters();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTAlloc
} // namespace NYT
