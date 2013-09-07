#pragma once

#include "public.h"
#include "delayed_invoker.h"

#include <ytlib/actions/invoker.h>
#include <ytlib/actions/callback.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Describes if manual calls to #TPeriodicInvoker::ScheduleNext are needed.
DECLARE_ENUM(EPeriodicInvokerMode,
    (Automatic)
    (Manual)
);

//! Helps to perform certain actions periodically.
class TPeriodicInvoker
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
    TPeriodicInvoker(
        IInvokerPtr invoker,
        TClosure callback,
        TDuration period,
        EPeriodicInvokerMode mode = EPeriodicInvokerMode::Automatic,
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
    EPeriodicInvokerMode Mode;
    TDuration Splay;

    TAtomic Started;
    TAtomic Busy;
    TAtomic OutOfBandRequested;
    TDelayedInvoker::TCookie Cookie;

    void PostDelayedCallback(TDuration delay);
    void PostCallback();

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
