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

TPeriodicInvoker::~TPeriodicInvoker()
{
    if (IsActive()) {
        Stop();
    }
}

bool TPeriodicInvoker::IsActive() const
{
    return CancelableInvoker->IsCanceled();
}

void TPeriodicInvoker::Start()
{
    RunAction();
}

void TPeriodicInvoker::Stop()
{
    YASSERT(IsActive());
    CancelableInvoker->Cancel();
    TDelayedInvoker::Get()->Cancel(Cookie);
    Cookie.Reset();
}

void TPeriodicInvoker::RunAction()
{
    Action->Do();
    Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TPeriodicInvoker::RunAction, TPtr(this))
        ->Via(~CancelableInvoker),
        Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
