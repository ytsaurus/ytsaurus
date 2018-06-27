#include "shutdown.h"

#include <yt/core/misc/shutdown.h>

#include <Objects.hxx> // pycxx

namespace NYT {
namespace NPython {

////////////////////////////////////////////////////////////////////////////////

static TCallback<void()> AdditionalShutdownCallback;

void Shutdown()
{
    AdditionalShutdownCallback.Run();
    AdditionalShutdownCallback.Reset();
    NYT::Shutdown();
}

void RegisterShutdown(TCallback<void()> additionalCallback)
{
    static bool registered = false;

    if (!registered) {
        AdditionalShutdownCallback = additionalCallback;
        registered = true;
        Py_AtExit(NPython::Shutdown);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NPython
} // namespace NYT

