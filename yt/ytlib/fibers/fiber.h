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

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TODO(babenko): move to public.h
class TFiber;
typedef TIntrusivePtr<TFiber> TFiberPtr;

DECLARE_ENUM(EFiberState,
    (Initialized) // Initialized, but not run.
    (Suspended) // Currently suspended.
    (Running) // Currently executing.
    (Terminated) // Terminated.
);

DECLARE_ENUM(EFiberStack,
    (Small)
    (Large)
);

// TODO(sandello): Proper support of exceptions in fibers.
// TODO(sandello): Substitutive yield.
class TFiber
    : public TIntrinsicRefCounted
{
private:
    TFiber();
    friend TIntrusivePtr<TFiber> New<TFiber>();

public:
    explicit TFiber(TClosure closure, EFiberStack stack = EFiberStack::Small);
    virtual ~TFiber();

    static TFiberPtr GetCurrent();
    static void SetCurrent(TFiberPtr fiber);

    static void Yield();

    DEFINE_BYVAL_RO_PROPERTY(EFiberState, State);

    void Run();
    void Reset(TClosure closure);
    void Inject(std::exception_ptr ex);

private:
    TClosure Callee;
    TFiberPtr Caller;

    std::exception_ptr Exception;

    coro_context CoroContext;
    coro_stack CoroStack;

    void SwitchTo(TFiber* target);

    static void Trampoline(void* arg);
};

////////////////////////////////////////////////////////////////////////////////

}
