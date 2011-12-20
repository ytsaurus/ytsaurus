#include "stdafx.h"
#include "periodic_invoker.h"

#include "../actions/action_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPeriodicInvoker::TPeriodicInvoker(IAction::TPtr action, TDuration period)
    : Action(action)
    , Period(period)
    , CancelableInvoker(New<TCancelableInvoker>(TSyncInvoker::Get()))
{ }

bool TPeriodicInvoker::IsActive() const
{
    return CancelableInvoker->IsCanceled();
}

void TPeriodicInvoker::Start()
{
    YASSERT(!IsActive());
    RunAction();
}

void TPeriodicInvoker::Stop()
{
    CancelableInvoker->Cancel();
    auto cookie = Cookie;
    if (cookie) {
        TDelayedInvoker::Cancel(cookie);
        Cookie.Reset();
    }
}

void TPeriodicInvoker::RunAction()
{
    Action->Do();
    Cookie = TDelayedInvoker::Submit(
        ~FromMethod(&TPeriodicInvoker::RunAction, TPtr(this))
        ->Via(~CancelableInvoker),
        Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
