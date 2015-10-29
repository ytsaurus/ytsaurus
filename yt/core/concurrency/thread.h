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

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
