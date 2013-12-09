#pragma once

#include "public.h"

#include <core/misc/error.h>

#include <core/actions/callback.h>
#include <core/actions/future.h>
#include <core/actions/invoker_util.h>

#include <exception>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

// Someone above has defined this by including one of Windows headers.
#undef Yield

////////////////////////////////////////////////////////////////////////////////

//! Thrown when a fiber is being terminated by external request.
class TFiberCanceledException
{ };

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFiberState,
    (Initialized) // Initialized, but not run.
    (Terminated)  // Terminated.
    (Canceled)    // Terminated because of a cancellation.
    (Exception)   // Terminated because of an exception.
    (Running)     // Currently executing.
    (Blocked)     // Blocked because control was transferred to another fiber.
    (Suspended)   // Suspended because control was transferred to an ancestor.
);

DECLARE_ENUM(EFiberStack,
    (Small)       // 256 Kb (default)
    (Large)       //   8 Mb
);

class TFiber
    : public TRefCounted
{
private:
    TFiber(const TFiber&);
    TFiber(TFiber&&);
    TFiber& operator=(const TFiber&);
    TFiber& operator=(TFiber&&);

    // A special constructor to create root fiber.
    TFiber();
    friend TFiberPtr NYT::New<TFiber>();
    friend void DestroyRootFiber();

public:
    explicit TFiber(TClosure callee, EFiberStack stack = EFiberStack::Small);
    ~TFiber();

    static TFiber* GetCurrent();

    static TFiber* GetExecutor();
    static void SetExecutor(TFiber* executor);

    EFiberState GetState() const;
    bool HasForked() const;
    bool IsCanceled() const;

    EFiberState Run();
    void Yield();
    void Yield(TFiber* caller);

    void Reset();
    void Reset(TClosure closure);

    void Inject(std::exception_ptr&& exception);
    void Cancel();

    IInvokerPtr GetCurrentInvoker() const;
    void SetCurrentInvoker(IInvokerPtr invoker);

    typedef intptr_t TFlsSlotValue;
    typedef TFlsSlotValue (*TFlsSlotCtor)();
    typedef void (*TFlsSlotDtor)(TFlsSlotValue);

    static int FlsRegister(TFlsSlotCtor ctor, TFlsSlotDtor dtor);
    TFlsSlotValue FlsGet(int index);
    void FlsSet(int index, TFlsSlotValue value);

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    friend void SwitchTo(IInvokerPtr invoker);
    friend void WaitFor(TFuture<void> future, IInvokerPtr invoker);

};

////////////////////////////////////////////////////////////////////////////////

//! Yields control to the caller (if |target| is not specified)
//! until it is manually transferred back to the executing fiber.
//!
//! Semantics are more complicated when |target| is specified.
//! It required that the target is an ancestor of currently executing fiber.
//! Yielding to the target fiber transfers control to target's caller
//! and when control is transferred back to the target it continues to execute
//! currently executing fiber.
//!
//! Effectively this suspends entire fiber chain descending from the target.
void Yield(TFiber* target = nullptr);

//! Transfers control to another invoker.
/*!
  *  The behavior is achieved by yielding control and enqueuing
  *  a special continuation callback into |invoker|.
  */
void SwitchTo(IInvokerPtr invoker);

//! Yields control until a given future is set and ensures that
//! execution continues within a given invoker.
void WaitFor(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Yields control until a given future is set, ensures that
//! execution continues within a given invoker, and returns
//! the final value of the future.
template <class T>
T WaitFor(
    TFuture<T> future,
    IInvokerPtr invoker = GetCurrentInvoker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define FIBER_INL_H_
#   include "fiber-inl.h"
#undef FIBER_INL_H_
