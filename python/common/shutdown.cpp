#include "shutdown.h"

#include <yt/core/misc/shutdown.h>

#include <Objects.hxx> // pycxx

#include <array>

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MaxAdditionalShutdownCallbackCount = 10;
static std::array<TCallback<void()>, MaxAdditionalShutdownCallbackCount> AdditionalShutdownCallbacks;

void Shutdown()
{
    for (int index = 0; index < MaxAdditionalShutdownCallbackCount; ++index) {
        auto& callback = AdditionalShutdownCallbacks[index];
        if (callback) {
            callback.Run();
            callback.Reset();
        }
    }
    NYT::Shutdown();
}

void RegisterShutdownCallback(TCallback<void()> additionalCallback, int index)
{
    YCHECK(0 <= index && index < MaxAdditionalShutdownCallbackCount);
    AdditionalShutdownCallbacks[index] = additionalCallback;
}

void RegisterShutdown()
{
    static bool registered = false;

    if (!registered) {
        registered = true;
        Py_AtExit(NPython::Shutdown);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

