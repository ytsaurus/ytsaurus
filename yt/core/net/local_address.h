#pragma once

#include "public.h"

namespace NYT::NNet {

////////////////////////////////////////////////////////////////////////////////

// Provides a bunch of functions to work with local hostname.
//
// If you are willing to change / extend these functions, please do it carefully.
// Read the comments, read the implementation, consult with the authors.
//
// Thanks!

// Read* & Write* functions work with statically allocated buffer.
// These functions are carefully engineered to be as safe and as robust
// as possible, because they may be called at any time (e. g. during
// invocation of atexit() hooks; or during static initialization).
// They must never throw / crash / corrupt program.
//
// Read* returns a pointer to a null-terminated string. Pointer is always valid
// (because it is a static memory!), string content never changes.
// Read* may be called concurrently, it is a lock-free function.
//
// Write* checks if the new value differs from the latest one, and saves
// new value to the static memory if so. New value becomes visible for future
// Read*s.
// Write* may be called concurrently, but it blocks.
const char* ReadLocalHostName() noexcept;
void WriteLocalHostName(const char* data, size_t length) noexcept;

// Get* & Set* wrap Read* & Write* for more convenient usage.
// The price is -- a dynamically allocated string.
TString GetLocalHostName();
void SetLocalHostName(const TString& hostname);

// Update* function interacts with the system to determine actual hostname
// of the local machine (by calling `gethostname` and `getaddrinfo`).
// On success, calls Write* with the hostname, and returns `true`.
// On failure, calls error callback with diagnostics, and returns `false`.
bool UpdateLocalHostName(std::function<void(const char*, const char*)> errorCb);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NNet
