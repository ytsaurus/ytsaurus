#pragma once

#include "public.h"

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Sets current thread name so it would be visible in utilities like top.
//! Note that name length is usually limited by 16 characters.
void SetCurrentThreadName(const char* name);

//! Caches current thread id in TLS and returns it (fast).
TThreadId GetCurrentThreadId();

//! Tries to increase the priority of the current thread up to the highest value
//! supported by the OS. Returns |true| on success, |false| on failure.
bool RaiseCurrentThreadPriority();

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
