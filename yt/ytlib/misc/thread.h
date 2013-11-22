#pragma once

#include <util/system/defaults.h>

namespace NYT {
namespace NThread {

////////////////////////////////////////////////////////////////////////////////

//! Sets current thread name so it would be visible in utilities like top.
//! Note that name length is usually limited by 16 characters.
void SetCurrentThreadName(const char* name);

typedef size_t TThreadId;
const size_t InvalidThreadId = 0;

//! Caches current thread id in TLS and returns it (fast).
TThreadId GetCurrentThreadId();

//! Tries to increase the priority of the current thread up to the highest value
//! supported by the OS. Returns |true| on success, |false| on failure.
bool RaiseCurrentThreadPriority();

////////////////////////////////////////////////////////////////////////////////

} // namespace NThread
} // namespace NYT
