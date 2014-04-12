#pragma once

#include "public.h"

#include <core/actions/future.h>

// Someone above has defined this by including one of Windows headers.
#undef Yield

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler holds a group of fibers executing on a particular thread
//! and provide means for cooperative multitasking on that thread.
struct IScheduler
{
    virtual ~IScheduler();

    virtual TFiber* GetCurrentFiber() = 0;

    //! Returns control back to the scheduler.
    //! This must be called upon fiber termination.
    virtual void Return() = 0;

    //! Transfers control back to the scheduler and reschedules currently
    //! executing fiber to the end of the run queue.
    virtual void Yield() = 0;

    //! Transfers control to other fiber and reschedules currently executing fiber
    //! to the end of the run queue.
    virtual void YieldTo(TFiberPtr&& other) = 0;

    //! Transfers control back to the scheduler and reschedules currently executing
    //! fiber via the specified invoker.
    virtual void SwitchTo(IInvokerPtr invoker) = 0;

    //! Transfers control back to the scheduler and puts currently executing fiber
    //! into sleep until occurence of an external event.
    virtual void WaitFor(TFuture<void> future, IInvokerPtr invoker) = 0;
};

////////////////////////////////////////////////////////////////////////////////
// Provides a way to work with the current scheduler.
// Scheduler is thread-scoped so this is an access to TLS.

//! Returns the current scheduler. Fails if there's none.
IScheduler* GetCurrentScheduler();

//! Returns the current scheduler or |nullptr| if there's none.
IScheduler* GetCurrentSchedulerUnsafe();

class TCurrentSchedulerGuard
{
public:
    explicit TCurrentSchedulerGuard(IScheduler* scheduler);
    ~TCurrentSchedulerGuard();

private:
    IScheduler* SavedScheduler_;
};

////////////////////////////////////////////////////////////////////////////////
// Shortcuts.

void Yield();

void SwitchTo(IInvokerPtr invoker);

void WaitFor(TFuture<void> future, IInvokerPtr invoker = GetCurrentInvoker());

template <class T>
T WaitFor(
    TFuture<T> future,
    IInvokerPtr invoker = GetCurrentInvoker())
{
    WaitFor(future.IgnoreResult(), std::move(invoker));
    YCHECK(future.IsSet());
    return future.Get();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT

