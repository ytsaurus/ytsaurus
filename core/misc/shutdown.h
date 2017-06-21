#pragma once

namespace NYT {

// Register callback to be called during Shutdown(). Callback are
// invoked from highest priority to lower. Invocation order for
// callback with the same priority is unspecified.
void RegisterShutdownCallback(double priority, void(*callback)());

void Shutdown();

////////////////////////////////////////////////////////////////////////////////

#define REGISTER_SHUTDOWN_CALLBACK(priority, callback) \
    static int dummy_shutdown ## __LINE__ = [] { \
        RegisterShutdownCallback(priority, callback); \
        return 0; \
    }();

} // namespace NYT
