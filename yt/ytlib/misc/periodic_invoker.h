#pragma once

#include "delayed_invoker.h"

#include <ytlib/actions/cancelable_context.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Performs action periodically.
/*!
 *  \note
 *  Uses TDelayedInvoker. Consider wrapping actions with #IAction::Via.
 */
class TPeriodicInvoker
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TPeriodicInvoker> TPtr;

    //! Initializes an instance.
    /*!
     *  \note
     *  We must call #Start to activate the instance.
     * 
     *  \param action An action to call.
     *  \param period An interval between consequent invocations.
     */
    TPeriodicInvoker(IAction::TPtr action, TDuration period);

    //! Starts invocations.
    /*!
     *  \note
     *  The first invocation of the action happens immediately.
     */
    void Start();

    //! Stops invocations.
    void Stop();

    //! Returns True iff the invoker is active.
    bool IsActive() const;

private:
    IAction::TPtr Action;
    TDuration Period;
    TDelayedInvoker::TCookie Cookie;
    TCancelableContextPtr CancelableContext;
    IInvoker::TPtr CancelableInvoker;

    void RunAction();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
