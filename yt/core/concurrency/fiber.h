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
class TFiberTerminatedException
{ };

//! Returns a pointer to a new TFiberTerminatedException instance.
std::exception_ptr CreateFiberTerminatedException();

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFiberState,
    (Initialized) // Initialized, but not run.
    (Terminated)  // Terminated.
    (Exception)   // Terminated because of an exception.
    (Suspended)   // Currently suspended.
    (Running)     // Currently executing.
);

DECLARE_ENUM(EFiberStack,
    (Small)       // 256 Kb (default)
    (Large)       //   8 Mb
);

class TFiber
    : public TRefCounted
{
private:
    TFiber();
    TFiber(const TFiber&);
    TFiber(TFiber&&);
    TFiber& operator=(const TFiber&);
    TFiber& operator=(TFiber&&);

    friend TIntrusivePtr<TFiber> NYT::New<TFiber>();

public:
    explicit TFiber(TClosure callee, EFiberStack stack = EFiberStack::Small);
    ~TFiber();

    static TFiber* GetCurrent();

    EFiberState GetState() const;
    bool Yielded() const;
    bool IsTerminating() const;
    bool IsCanceled() const;

    EFiberState Run();
    void Yield();

    void Reset();
    void Reset(TClosure closure);

    void Inject(std::exception_ptr&& exception);
    void Cancel();

    void SwitchTo(IInvokerPtr invoker);
    void WaitFor(TFuture<void> future, IInvokerPtr invoker);

    IInvokerPtr GetCurrentInvoker();
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

};

////////////////////////////////////////////////////////////////////////////////

//! Yields control until it is manually transferred back to the current fiber.
void Yield();

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

//! Transfers control to another invoker.
/*!
  *  The behavior is achieved by yielding control and enqueuing
  *  a special continuation callback into |invoker|.
  */
void SwitchTo(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

#define FIBER_INL_H_
#   include "fiber-inl.h"
#undef FIBER_INL_H_
