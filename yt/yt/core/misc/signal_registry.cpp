#include "signal_registry.h"

#include <yt/build/config.h>

#ifdef HAVE_PTHREAD_H
#   include <pthread.h>
#endif

#include <signal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TSignalRegistry* TSignalRegistry::Get()
{
    return Singleton<TSignalRegistry>();
}

void TSignalRegistry::SetupSignal(int signal, int flags)
{
#ifdef _unix_
    DispatchMultiSignal(signal, [&] (int signal) {
        if (signal == SIGALRM) {
            // Why would you like to use SIGALRM? It is used in crash handler
            // to prevent program hunging, do not interfere.
            YT_VERIFY(false);
        }

        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sigemptyset(&sa.sa_mask);
        sa.sa_flags = flags | SA_SIGINFO;
        sa.sa_sigaction = &Handle;
        YT_VERIFY(sigaction(signal, &sa, NULL) == 0);
        Signals_[signal].SetUp = true;
    });
#endif
}

void TSignalRegistry::PushCallback(int signal, std::function<void(int, siginfo_t*, void*)> callback)
{
    DispatchMultiSignal(signal, [&] (int signal) {
        if (!Signals_[signal].SetUp) {
            SetupSignal(signal);
        }
        Signals_[signal].Callbacks.emplace_back(callback);
    });
}

void TSignalRegistry::PushCallback(int signal, std::function<void(int)> callback)
{
    PushCallback(signal, [callback = std::move(callback)] (int signal, siginfo_t* /* siginfo */, void* /* ucontext */) {
        callback(signal);
    });
}

void TSignalRegistry::PushCallback(int signal, std::function<void(void)> callback)
{
    PushCallback(signal, [callback = std::move(callback)] (int /* signal */, siginfo_t* /* siginfo */, void* /* ucontext */) {
        callback();
    });
}

void TSignalRegistry::PushDefaultSignalHandler(int signal)
{
    PushCallback(signal, [] (int signal) {
        struct sigaction sa;
        memset(&sa, 0, sizeof(sa));
        sigemptyset(&sa.sa_mask);
        sa.sa_handler = SIG_DFL;
        YT_VERIFY(sigaction(signal, &sa, nullptr) == 0);

        pthread_kill(pthread_self(), signal);
    });
}

void TSignalRegistry::Handle(int signal, siginfo_t* siginfo, void* ucontext)
{
    auto* self = Get();
    for (const auto& callback : self->Signals_[signal].Callbacks) {
        callback(signal, siginfo, ucontext);
    }
}

template <class TCallback>
void TSignalRegistry::DispatchMultiSignal(int multiSignal, const TCallback& callback)
{
    std::vector<int> signals;
    if (multiSignal == AllCrashSignals) {
        signals = {SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS};
    } else {
        signals = {multiSignal};
    }

    for (int signal : signals) {
        callback(signal);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
