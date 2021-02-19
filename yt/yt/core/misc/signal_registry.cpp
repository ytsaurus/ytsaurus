#include "signal_registry.h"

#include <yt/build/config.h>

#ifdef HAVE_PTHREAD_H
#   include <pthread.h>
#endif

#include <signal.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

std::vector<int> CrashSignals = {SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS};

////////////////////////////////////////////////////////////////////////////////

// This variable is used for protecting signal handlers for crash signals from
// dumping stuff while another thread is already doing that. Our policy is to let
// the first thread dump stuff and make other threads wait.
std::atomic<pthread_t*> CrashingThreadId;

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

    if (self->EnableCrashSignalProtection_ &&
        std::find(CrashSignals.begin(), CrashSignals.end(), signal) != CrashSignals.end()) {
        // For crash signals we try pretty hard to prevent simultaneous execution of
        // several crash handlers.

        // We assume pthread_self() is async signal safe, though it's not
        // officially guaranteed.
        auto currentThreadId = pthread_self();
        // NOTE: We could simply use pthread_t rather than pthread_t* for this,
        // if pthread_self() is guaranteed to return non-zero value for thread
        // ids, but there is no such guarantee. We need to distinguish if the
        // old value (value returned from __sync_val_compare_and_swap) is
        // different from the original value (in this case NULL).
        pthread_t* expectedCrashingThreadId = nullptr;
        if (!CrashingThreadId.compare_exchange_strong(expectedCrashingThreadId, &currentThreadId)) {
            // We've already entered the signal handler. What should we do?
            if (pthread_equal(currentThreadId, *expectedCrashingThreadId)) {
                // It looks the current thread is reentering the signal handler.
                // Something must be going wrong (maybe we are reentering by another
                // type of signal?). Simply return from here and hope that the default signal handler
                // (which is going to be executed after us by TSignalRegistry) will succeed in killing us.
                // Otherwise, we will probably end up  running out of stack entering
                // CrashSignalHandler over and over again. Not a bad thing, after all.
                return;
            } else {
                // Another thread is dumping stuff. Let's wait until that thread
                // finishes the job and kills the process.
                while (true) {
                    sleep(1);
                }
            }
        }

        // This is the first time we enter the signal handler.
        // Let the rest of the handlers do their interesting stuff.
    }

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
