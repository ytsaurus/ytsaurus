#pragma once

#include <util/system/types.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TUndumpableMark;

//! Add byte range to undumpable set.
TUndumpableMark* MarkUndumpable(void* ptr, size_t size);

//! Remove byte range from undumpable set.
void UnmarkUndumpable(TUndumpableMark* tag);

//! Add byte range to undumpable set.
/**
 *  Unlike MarkUndumpable, this method does not require user to keep pointer to TUndumpableMark*.
 */
void MarkUndumpableOOB(void* ptr, size_t size);

//! Remove byte range from undumpable set.
void UnmarkUndumpableOOB(void* ptr);

//! GetUndumpableBytesCount return total size of undumpable set.
size_t GetUndumpableBytesCount();

//! GetMemoryFootprint returns estimate of memory consumed by internal data structures.
size_t GetUndumpableMemoryFootprint();

//! CutUndumpableFromCoredump call's madvice(MADV_DONTNEED) for all current undumpable objects.
/**
 *  This function is async-signal safe. Usually, this function should be called from segfault handler.
 */
void CutUndumpableFromCoredump();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
