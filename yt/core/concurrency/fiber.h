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
    // A special constructor to create root fiber.
    TFiber();
    friend TFiberPtr NYT::New<TFiber>();

public:
    explicit TFiber(TClosure callee, EFiberStack stack = EFiberStack::Small);

    TFiber(const TFiber&) = delete;
    TFiber(TFiber&&) = delete;

    ~TFiber();

    // Thread-local information.
    static void InitTls();
    static void FiniTls();

    static TFiber* GetCurrent();

    static TFiber* GetExecutor();
    static void SetExecutor(TFiber* executor);

    // Fiber-local information.
    typedef void* TFlsSlotValue;
    typedef TFlsSlotValue (*TFlsSlotCtor)();
    typedef void (*TFlsSlotDtor)(TFlsSlotValue);

    static int FlsAllocateSlot(TFlsSlotCtor ctor, TFlsSlotDtor dtor);
    TFlsSlotValue FlsGet(int index);
    void FlsSet(int index, TFlsSlotValue value);

    // Fiber interface.
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

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    friend void SwitchTo(IInvokerPtr invoker);
    friend void WaitFor(TFuture<void> future, IInvokerPtr invoker);

};

////////////////////////////////////////////////////////////////////////////////

//! Yields control until it is manually transferred back to the executing fiber.
/*!
 *  If |target| is omitted is is taked to be the caller of the executing fiber.
 *
 *  In general case it is required that the target is an ancestor of
 *  the executing fiber. Yielding to the target transfers control
 *  to target's caller and when control is transferred back to the target
 *  the currently executing fiber is resumed.
 *
 *  Effectively this suspends entire fiber stack descending from the target.
 */
void Yield(TFiber* target = nullptr);

//! Transfers current fiber to another invoker.
/*!
  *  The behavior is achieved by yielding control and enqueuing
  *  a special continuation callback into |invoker|.
  */
void SwitchTo(IInvokerPtr invoker);

//! Yields control until a given future is set and ensures that
//! execution resumes within a given invoker.
void WaitFor(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Yields control until a given future is set, ensures that
//! execution resumes within a given invoker, and returns
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
