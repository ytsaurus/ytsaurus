#pragma once

#include "public.h"
#include "delayed_executor.h"

#include <core/actions/invoker.h>
#include <core/actions/callback.h>

namespace NYT {
namespace NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Describes if manual calls to #TPeriodicExecutor::ScheduleNext are needed.
DECLARE_ENUM(EPeriodicExecutorMode,
    (Automatic)
    (Manual)
);

//! Helps to perform certain actions periodically.
class TPeriodicExecutor
    : public TRefCounted
{
public:
    //! Initializes an instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     *
     *  \param invoker Invoker used for wrapping actions.
     *  \param callback Callback to invoke periodically.
     *  \param period Interval between usual consequent invocations.
     *  \param splay First invocation splay time.
     */
    TPeriodicExecutor(
        IInvokerPtr invoker,
        TClosure callback,
        TDuration period,
        EPeriodicExecutorMode mode = EPeriodicExecutorMode::Automatic,
        TDuration splay = TDuration::Zero());

    //! Starts the instance.
    //! The first invocation happens with a random delay within splay time.
    void Start();

    //! Stops the instance, cancels all subsequent invocations.
    void Stop();

    //! Requests an immediate invocation.
    void ScheduleOutOfBand();

    //! Usually called from the callback to schedule the next invocation.
    void ScheduleNext();

private:
    IInvokerPtr Invoker;
    TClosure Callback;
    TDuration Period;
    EPeriodicExecutorMode Mode;
    TDuration Splay;

    TSpinLock SpinLock;
    bool Started;
    bool Busy;
    bool OutOfBandRequested;
    TDelayedExecutorCookie Cookie;

    void PostDelayedCallback(TDuration delay);
    void PostCallback();

    void OnCallbackSuccess();
    void OnCallbackFailure();

};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NConcurrency
} // namespace NYT
