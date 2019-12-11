#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Shorthand for all crash signals (SIGSEGV, SIGILL, SIGFPE, SIGABRT, SIGBUS).
//! May be used instead of signal in all public methods of signal registry.
constexpr int AllCrashSignals = -1;

//! Singleton class which provides convenient interface for signal handler registration.
class TSignalRegistry
{
public:
    static TSignalRegistry* Get();

    //! Setup our handler that invokes registered callbacks in order.
    //! Flags has same meaning as sa_flags in sigaction(2). Use this method if you need certain flags.
    //! By default any signal touched by PushCallback(...) will be set up with default flags.
    void SetupSignal(int signal, int flags = 0);

    //! Add simple callback which should be called for signal. Different signatures are supported for convenience.
    void PushCallback(int signal, std::function<void(void)> callback);
    void PushCallback(int signal, std::function<void(int)> callback);
    void PushCallback(int signal, std::function<void(int, siginfo_t*, void*)> callback);

    //! Add default signal handler which is called after invoking our custom handlers.
    //! NB: this handler restores default signal handler as a side-effect. Use it only
    //! when default handler terminates the program.
    void PushDefaultSignalHandler(int signal);

private:
    static constexpr int SignalRange = 64;

    struct TSignalSetup
    {
        std::vector<std::function<void(int, siginfo_t*, void*)>> Callbacks;
        bool SetUp = false;
    };
    std::array<TSignalSetup, SignalRange> Signals_;

    static void Handle(int signal, siginfo_t* siginfo, void* ucontext);

    //! Invoke something for `multisignal` which may either be some real signal or signal set like `AllCrashSignals`.
    template <class TCallback>
    void DispatchMultiSignal(int multiSignal, const TCallback& callback);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
