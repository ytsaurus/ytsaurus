#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/property.h>

#include <ytlib/actions/callback.h>

#if defined(_unix_) && !defined(CORO_ASM)
#    error "Using slow libcoro backend (expecting CORO_ASM)"
#endif
#if defined(_win_)
#    if !defined(CORO_FIBER)
#        error "Using slow libcoro backend (expecting CORO_FIBER)"
#    endif
// You like WinAPI, don't you? :)
#    undef Yield
#endif

#include <contrib/libcoro/coro.h>

#include <exception>
#include <stdexcept>

// MSVC compiler has /GT option for supporting fiber-safe thread-local storage.
// For CXXABIv1-compliant systems we can hijack __cxa_eh_globals.
// See http://mentorembedded.github.io/cxx-abi/abi-eh.html
#if defined(__GNUC__) || defined(__clang__)
#define CXXABIv1
#ifdef HAVE_CXXABI_H
#include <cxxabi.h>
#endif
namespace __cxxabiv1 {
    // We do not care about actual type here, so erase it.
    typedef void __cxa_exception;
    struct __cxa_eh_globals {
        __cxa_exception* caughtExceptions;
        unsigned int uncaughtExceptions;
    };
    extern "C" __cxa_eh_globals* __cxa_get_globals() throw();
} // namespace __cxxabiv1
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to public.h
class TFiber;
typedef TIntrusivePtr<TFiber> TFiberPtr;

DECLARE_ENUM(EFiberState,
    (Initialized) // Initialized, but not run.
    (Terminated) // Terminated.
    (Exception) // Terminated because of exception.
    (Suspended) // Currently suspended.
    (Running) // Currently executing.
);

DECLARE_ENUM(EFiberStack,
    (Small)
    (Large)
);

class TFiberExceptionHandler
{
public:
    TFiberExceptionHandler();
    ~TFiberExceptionHandler();
    void Swap(TFiberExceptionHandler& other);
private:
#ifdef CXXABIv1
    __cxxabiv1::__cxa_eh_globals EH;
#endif
};

// TODO(sandello): Substitutive yield.
class TFiber
    : public TIntrinsicRefCounted
{
private:
    TFiber();
    TFiber(const TFiber&);
    TFiber(TFiber&&);
    TFiber& operator=(const TFiber&);
    TFiber& operator=(TFiber&&);

    friend TIntrusivePtr<TFiber> New<TFiber>();

public:
    explicit TFiber(TClosure closure, EFiberStack stack = EFiberStack::Small);
    virtual ~TFiber();

    static TFiberPtr GetCurrent();
    static void SetCurrent(const TFiberPtr& fiber);
    static void SetCurrent(TFiberPtr&& fiber);
    static void Yield();

    DEFINE_BYVAL_RO_PROPERTY(EFiberState, State);

    void Run();
    void Reset();
    void Reset(TClosure closure);
    void Inject(std::exception_ptr&& exception);

private:
    TClosure Callee;
    TFiberPtr Caller;

    coro_context CoroContext;
    coro_stack CoroStack;

    std::exception_ptr Exception;
    TFiberExceptionHandler EH;

    void SwitchTo(TFiber* target);

    static void Trampoline(void* arg);
};

////////////////////////////////////////////////////////////////////////////////

}
