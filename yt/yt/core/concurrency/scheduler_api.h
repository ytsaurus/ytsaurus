#pragma once

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// Forward declaration
IInvokerPtr GetCurrentInvoker();

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

using TFiberCanceler = TCallback<void(const TError&)>;

TFiberCanceler GetCurrentFiberCanceler();

////////////////////////////////////////////////////////////////////////////////

//! Returns the current fiber id.
TFiberId GetCurrentFiberId();

//! Sets the current fiber id.
void SetCurrentFiberId(TFiberId id);

////////////////////////////////////////////////////////////////////////////////

void PushContextHandler(std::function<void()> out, std::function<void()> in);
void PopContextHandler();

class TContextSwitchGuard
{
public:
    TContextSwitchGuard(std::function<void()> out, std::function<void()> in);
    TContextSwitchGuard(const TContextSwitchGuard& other) = delete;
    ~TContextSwitchGuard();
};

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

void WaitUntilSet(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(
    TFuture<T> future,
    IInvokerPtr invoker = GetCurrentInvoker());

template <class T>
[[nodiscard]] TErrorOr<T> WaitForUnique(
    const TFuture<T>& future,
    IInvokerPtr invoker = GetCurrentInvoker());

void Yield();

void SwitchTo(IInvokerPtr invoker);

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

struct IScheduler
{
    virtual ~IScheduler() = default;

    //! Transfers control back to the scheduler and puts currently executing fiber
    //! into sleep until occurrence of an external event.
    virtual void WaitUntilSet(TFuture<void> future, IInvokerPtr invoker) = 0;

};

//! Sets the current scheduler. Can only be called once per thread.
void SetCurrentScheduler(IScheduler* scheduler);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

#define SCHEDULER_API_INL_H_
#include "scheduler_api-inl.h"
#undef SCHEDULER_API_INL_H_
