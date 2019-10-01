#pragma once

#include <yt/core/misc/common.h>

#include <yt/core/actions/invoker_util.h>
#include <yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
[[nodiscard]] TErrorOr<T> WaitFor(TFuture<T> future, IInvokerPtr invoker = GetCurrentInvoker());

inline void Yield();

inline void SwitchTo(IInvokerPtr invoker);

////////////////////////////////////////////////////////////////////////////////

struct IScheduler
{
    virtual ~IScheduler() = default;

    //! Transfers control back to the scheduler and puts currently executing fiber
    //! into sleep until occurrence of an external event.
    virtual void WaitFor(TAwaitable awaitable, IInvokerPtr invoker) = 0;

};

//! Sets the current scheduler. Can only be called once per thread.
void SetCurrentScheduler(IScheduler* scheduler);

////////////////////////////////////////////////////////////////////////////////

} //namespace NYT::NConcurrency

#define SCHEDULER_API_INL_H_
#include "scheduler_api-inl.h"
#undef SCHEDULER_API_INL_H_
