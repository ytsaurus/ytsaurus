#pragma once

#include "delayed_invoker.h"

namespace NYT {

//! Performs action periodically
/*
 * \note Uses TDelayedInvoker. Wrap actions with Via(...)
 */
class TPeriodicInvoker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TPeriodicInvoker> TPtr;

    TPeriodicInvoker(IAction::TPtr action, TDuration period);
    ~TPeriodicInvoker();

    bool IsActive() const;

    /*
     * \note Performs action immediately
     */
    void Start();
    void Stop();

private:
    IAction::TPtr Action;
    TDuration Period;
    TDelayedInvoker::TCookie Cookie;

    void PerformAction();
};

} // namespace NYT
