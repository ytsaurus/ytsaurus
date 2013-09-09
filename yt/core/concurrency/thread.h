#pragma once

#include "public.h"

// Parts of our code currently reside in util/system/thread.h.
#include <util/system/thread.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Tries to increase the priority of the current thread up to the highest value
//! supported by the OS. Returns |true| on success, |false| on failure.
bool RaiseCurrentThreadPriority();

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
