#pragma once

#include "public.h"

#include <yt/core/actions/future.h>

// Someone above has defined this by including one of Windows headers.
#undef Yield

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler holds a group of fibers executing on a particular thread
//! and provide means for cooperative multitasking on that thread.
struct IScheduler
{
    virtual ~IScheduler() = default;

    virtual TFiber* GetCurrentFiber() = 0;

    //! Returns control back to the scheduler.
    //! This must be called upon fiber termination.
    virtual void Return() = 0;

    //! Transfers control to other fiber and reschedules currently executing fiber
    //! to the end of the run queue.
    virtual void YieldTo(TFiberPtr&& other) = 0;

    //! Transfers control back to the scheduler and reschedules currently executing
    //! fiber via the specified invoker.
    virtual void SwitchTo(IInvokerPtr invoker) = 0;

    //! Installs a new context switch handlers.
    /*!
     *  Specified handlers are invoked during context switching inside the fiber.
     *  #out handler is invoked on context switch from current fiber.
     *  #in handler is invoked on context switch to current fiber.
     *
     *  Those handlers are always invoked in specified cases until #PopInterceptHandlers is invoked.
     */
    virtual void PushContextSwitchHandler(std::function<void()> out, std::function<void()> in) = 0;

    //! Removes the top switch handlers.
    virtual void PopContextSwitchHandler() = 0;

    //! Transfers control back to the scheduler and puts currently executing fiber
    //! into sleep until occurrence of an external event.
    virtual void WaitFor(TFuture<void> future, IInvokerPtr invoker) = 0;

};

////////////////////////////////////////////////////////////////////////////////
// Provides a way to work with the current scheduler and fiber.
// Schedulers and fibers are thread-scoped so this is an access to TLS.

//! Returns the current scheduler. Fails if there's none.
IScheduler* GetCurrentScheduler();

//! Returns the current scheduler or |nullptr| if there's none.
IScheduler* TryGetCurrentScheduler();

//! Sets the current scheduler. Can only be called once per thread.
void SetCurrentScheduler(IScheduler* scheduler);

//! Generates a fresh fiber id.
TFiberId GenerateFiberId();

//! Returns the current fiber id.
TFiberId GetCurrentFiberId();

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id);

////////////////////////////////////////////////////////////////////////////////
// Shortcuts.

void Yield();
void SwitchTo(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(std::function<void()> out, std::function<void()> in);
    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

class TOneShotContextSwitchGuard
    : public TContextSwitchGuard
{
public:
    explicit TOneShotContextSwitchGuard(std::function<void()> handler);

private:
    bool Active_;

};

class TForbidContextSwitchGuard
    : public TOneShotContextSwitchGuard
{
public:
    TForbidContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

#ifndef FAKEID
#define WF_WARN_UNUSED_RESULT Y_WARN_UNUSED_RESULT
#else
#define WF_WARN_UNUSED_RESULT
#endif

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future) WF_WARN_UNUSED_RESULT;

template <class T>
TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker) WF_WARN_UNUSED_RESULT;

////////////////////////////////////////////////////////////////////////////////

//! Thrown when a fiber is being terminated by an external event.
class TFiberCanceledException
{ };

//! Delegates to TFiber::GetCanceler for the current fiber.
//! Used to avoid dependencies on |fiber.h|.
TClosure GetCurrentFiberCanceler();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency

#define SCHEDULER_INL_H_
#include "scheduler-inl.h"
#undef SCHEDULER_INL_H_
