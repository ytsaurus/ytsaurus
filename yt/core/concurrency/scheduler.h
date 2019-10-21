#pragma once

#include "public.h"

#include <yt/core/actions/future.h>
#include <yt/core/actions/invoker_util.h>

namespace NYT {

IInvokerPtr GetCurrentInvoker();

} // namespace NYT

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Scheduler holds a group of fibers executing on a particular thread
//! and provide means for cooperative multitasking on that thread.
struct IScheduler
{
    virtual ~IScheduler() = default;

    //! Returns control back to the scheduler.
    //! This must be called upon fiber termination.
    virtual void Return() = 0;

    //! Transfers control to other fiber and reschedules currently executing fiber
    //! to the end of the run queue.
    virtual void YieldTo(TFiberPtr&& other) = 0;

    //! Transfers control back to the scheduler and reschedules currently executing
    //! fiber via the specified invoker.
    virtual void SwitchTo(IInvokerPtr invoker) = 0;

    //! Transfers control back to the scheduler and puts currently executing fiber
    //! into sleep until occurrence of an external event.
    virtual void WaitFor(TAwaitable awaitable, IInvokerPtr invoker) = 0;
};

//! Yield from current fiber.
void Yield();

//! Switch execution (current fiber) to another invoker.
void SwitchTo(IInvokerPtr invoker);

//! Exit from current fiber.
void ReturnFromFiber();

//! Suspend current fiber and continue execution of another one.
void YieldToFiber(TFiberPtr&& other);

////////////////////////////////////////////////////////////////////////////////
// Provides a way to work with the current invoker, scheduler, and fiber.
// Invokers, schedulers and fibers are thread-scoped so this is an access to TLS.

//! Returns the current invoker.
IInvokerPtr GetCurrentInvokerImpl();

//! Sets the current invoker.
void SetCurrentInvoker(IInvokerPtr invoker);

//! Sets the current scheduler. Can only be called once per thread.
void SetCurrentScheduler(IScheduler* scheduler);

//! Returns the current fiber id.
TFiberId GetCurrentFiberId();

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id);

//! Returns the current fiber or |nullptr| if there's none.
TFiber* TryGetCurrentFiber();

//! Sets the current fiber id.
void SetCurrentFiber(TFiber* fiber);

////////////////////////////////////////////////////////////////////////////////

//! Returns the duration the fiber is running.
//! This counts CPU wall time but excludes periods the fiber was sleeping.
//! The call only makes sense if the fiber is currently runnning.
NProfiling::TCpuDuration GetCurrentRunCpuTime();

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(std::function<void()> out, std::function<void()> in);
    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

//! Swaps the current active invoker with a provided one.
class TCurrentInvokerGuard
    : NConcurrency::TContextSwitchGuard
{
public:
    explicit TCurrentInvokerGuard(IInvokerPtr invoker);
    ~TCurrentInvokerGuard();

private:
    void Restore();

    bool Active_;
    IInvokerPtr SavedInvoker_;
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

////////////////////////////////////////////////////////////////////////////////

class TForbidContextSwitchGuard
    : public TOneShotContextSwitchGuard
{
public:
    TForbidContextSwitchGuard();
};

////////////////////////////////////////////////////////////////////////////////

template <class T>
[[nodisard]] TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker = NYT::GetCurrentInvoker());

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
