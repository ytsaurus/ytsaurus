#pragma once

#include <yt/yt/core/misc/common.h>

#include <yt/yt/core/actions/future.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////
// Forward declaration
IInvokerPtr GetCurrentInvoker();

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

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

//! Blocks the current fiber until #future is set.
//! The fiber is resceduled to #invoker.
void WaitUntilSet(
    TFuture<void> future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Blocks the current fiber until #future is set and returns the resulting value.
//! The fiber is resceduled via #invoker.
template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(
    const TFuture<T>& future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Similar to #WaitFor but extracts the value from #future via |GetUnique|.
template <class T>
[[nodiscard]] TErrorOr<T> WaitForUnique(
    const TFuture<T>& future,
    IInvokerPtr invoker = GetCurrentInvoker());

//! Reschedules the current fiber via the current invoker.
void Yield();

//! Reschedules the current fiber via #invoker.
void SwitchTo(IInvokerPtr invoker);

//! Returns |true| if there is enough remaining stack space.
bool CheckFreeStackSpace(size_t space);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

#define SCHEDULER_API_INL_H_
#include "scheduler_api-inl.h"
#undef SCHEDULER_API_INL_H_
