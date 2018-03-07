#pragma once

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Registers a callback to be called during Shutdown(). Callback are
//! invoked from highest priority to lower. Invocation order for
//! callback with the same priority is unspecified.
void RegisterShutdownCallback(double priority, void(*callback)());

//! Invokes all registered shutdown callbacks.
void Shutdown();

//! Tells whether the shutdown process has already started.
bool IsShutdownStarted();

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_SHUTDOWN_CALLBACK(priority, callback) \
    static int dummy_shutdown ## __LINE__ __attribute__((unused)) = [] { \
        RegisterShutdownCallback(priority, callback); \
        return 0; \
    }();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
