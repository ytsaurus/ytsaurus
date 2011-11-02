#pragma once

#include "delayed_invoker.h"
#include "../actions/invoker_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Performs action periodically.
/*!
 *  \note
 *  Uses TDelayedInvoker. Consider wrapping actions with #IAction::Via.
 */
class TPeriodicInvoker
    : public TRefCountedBase
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
    ~TPeriodicInvoker();

    bool IsActive() const;

    //! Starts invocations.
    /*!
     *  \note
     *  The first invocation of the action happens immediately.
     */
    void Start();

    //! Stops invocations.
    void Stop();

private:
    IAction::TPtr Action;
    TDuration Period;
    TDelayedInvoker::TCookie Cookie;
    TCancelableInvoker::TPtr CancelableInvoker;

    void RunAction();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
