#pragma once

#include <ytlib/misc/common.h>
#include <ytlib/misc/property.h>
#include <ytlib/actions/callback.h>

#include <contrib/libcoro/coro.h>
#ifdef _unix_
#ifndef CORO_ASM
#error "Using slow libcoro backend."
#endif
#endif

#include <exception>
#include <stdexcept>

namespace NYT {

class TFiber;
typedef TIntrusivePtr<TFiber> TFiberPtr;

// TODO(sandello): Proper support of exceptions in fibers.
// TODO(sandello): Substitutive yield.
class TFiber
    : public TIntrinsicRefCounted
{
private:
    TFiber();
    friend TIntrusivePtr<TFiber> New<TFiber>();

public:
    DECLARE_ENUM(EState,
        (Initialized) // Initialized, but not run.
        (Suspended) // Currently suspended.
        (Running) // Currently executing.
        (Terminated) // Terminated.
    );

    DEFINE_BYVAL_RO_PROPERTY(EState, State);

public:
    enum EStack {
        SmallStack,
        LargeStack
    };

    TFiber(TClosure closure, EStack stack = SmallStack);
    virtual ~TFiber();

    static TFiberPtr GetCurrent();
    static void SetCurrent(TFiberPtr fiber);

    static void Yield();

    void Run();
    void Reset(TClosure closure);
    void Inject(std::exception_ptr ex);

private:
    TClosure Callee;
    TFiberPtr Caller;

    std::exception_ptr Exception;

    coro_context CoroContext;
    coro_stack CoroStack;

    void SwitchTo(TFiber& target);
    void SwitchTo(const TFiberPtr& target);

    static void Trampoline(void* arg);
};

}

