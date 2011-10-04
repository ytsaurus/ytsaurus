#pragma once

#include "delayed_invoker.h"
#include "../actions/invoker_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Performs action periodically.
/*!
 *  \note Uses TDelayedInvoker. Consider wrapping actions with #Via.
 */
class TPeriodicInvoker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TPeriodicInvoker> TPtr;

    TPeriodicInvoker(IAction::TPtr action, TDuration period);
    ~TPeriodicInvoker();

    bool IsActive() const;

    /*!
     *  \note Performs action immediately.
     */
    void Start();
    void Stop();

private:
    IAction::TPtr Action;
    TDuration Period;
    TDelayedInvoker::TCookie Cookie;
    TCancelableInvoker::TPtr CancelableInvoker;

    void PerformAction();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
