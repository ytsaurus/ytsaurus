#pragma once

// Parts of our code currently reside in util/system/thread.h.
#include <util/system/thread.h>

namespace NYT {
namespace NThread {

////////////////////////////////////////////////////////////////////////////////

//! Tries to increase the priority of the current thread up to the highest value
//! supported by the OS. Returns |true| on success, |false| on failure.
bool RaiseCurrentThreadPriority();
    
////////////////////////////////////////////////////////////////////////////////

} // namespace NThread
} // namespace NYT
