#pragma once

#include "public.h"

#include <yt/yt/core/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An opaque ref-counted entity representing a registered shutdown callback.
using TShutdownCookie = TIntrusivePtr<TRefCounted>;

//! Registers a new callback to be called at global shutdown.
/*!
 *  If null is returned then the shutdown has already been started
 *  and #callback is not registered.
 *
 *  When the returned cookie is lost, the callback is automatically
 *  unregistered.
 */
[[nodiscard]]
TShutdownCookie RegisterShutdownCallback(
    TString name,
    TClosure callback,
    int priority = 0);

//! Starts the global shutdown.
/*!
 *  Invokes all registered shutdown callbacks in the order of
 *  decreasing priority.
 *
 *  Safe to call multiple times. All calls after the first one are,
 *  howerver, no ops.
 */
void Shutdown();

//! Returns true if the global shutdown has already been started
//! (and is possibly already completed).
bool IsShutdownStarted();

//! Enables logging shutdown messages to stderr.
void EnableShutdownLoggingToStderr();

//! Enables logging shutdown messages to the given file.
void EnableShutdownLoggingToFile(const TString& fileName);

//! Returns the pointer to the log file if shutdown logging has been enabled or nullptr otherwise.
FILE* GetShutdownLogFile();

//! In case the global shutdown has been started, returns
//! the id of the thread invoking shutdown callbacks.
size_t GetShutdownThreadId();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
