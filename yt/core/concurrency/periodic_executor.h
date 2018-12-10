#pragma once

#include "public.h"
#include "delayed_executor.h"

#include <yt/core/actions/callback.h>
#include <yt/core/actions/future.h>

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

//! Describes if manual calls to #TPeriodicExecutor::ScheduleNext are needed.
DEFINE_ENUM(EPeriodicExecutorMode,
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
    //! Returns a future that becomes set when all outstanding callback
    //! invocations are finished and no more invocations are expected to happen.
    TFuture<void> Stop();

    //! Requests an immediate invocation.
    void ScheduleOutOfBand();

    //! Usually called from the callback to schedule the next invocation.
    void ScheduleNext();

    //! Changes execution period.
    void SetPeriod(TDuration period);

    //! Returns the future that become set when
    //! at least one action be fully executed from the moment of method call.
    TFuture<void> GetExecutedEvent();

private:
    const IInvokerPtr Invoker_;
    const TClosure Callback_;
    TDuration Period_;
    const EPeriodicExecutorMode Mode_;
    const TDuration Splay_;

    TSpinLock SpinLock_;
    bool Started_ = false;
    bool Busy_ = false;
    bool OutOfBandRequested_ = false;
    bool ExecutingCallback_ = false;
    TDelayedExecutorCookie Cookie_;
    TPromise<void> IdlePromise_;
    TPromise<void> ExecutedPromise_;

    void DoStop();

    TError GetStoppedError();

    void InitIdlePromise();
    void InitExecutedPromise();

    void PostDelayedCallback(TDuration delay);
    void PostCallback();

    void OnTimer(bool aborted);
    void OnCallbackSuccess();
    void OnCallbackFailure();

};

DEFINE_REFCOUNTED_TYPE(TPeriodicExecutor)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
