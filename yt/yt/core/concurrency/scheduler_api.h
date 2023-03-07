#pragma once

#include <yt/core/misc/common.h>

#include <yt/core/actions/invoker_util.h>
#include <yt/core/actions/future.h>

namespace NYT::NConcurrency {

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
