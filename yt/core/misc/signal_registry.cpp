#include "signal_registry.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSignalRegistry* TSignalRegistry::Get()
{
    return Singleton<TSignalRegistry>();
}

void TSignalRegistry::RegisterHandler(int signal, TCallback<void(void)> callback)
{
    Handlers_[signal] = callback;
    struct sigaction sa {
        .sa_handler = &Handle,
        .sa_flags = SA_RESTART,
    };
    sigaction(signal, &sa, NULL);
}

void TSignalRegistry::Handle(int signal)
{
    Get()->Handlers_[signal].Run();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
