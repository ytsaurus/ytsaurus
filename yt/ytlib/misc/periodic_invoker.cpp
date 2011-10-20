#include "../misc/stdafx.h"
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
    PerformAction();
}

void TPeriodicInvoker::Stop()
{
    YASSERT(IsActive());
    CancelableInvoker->Cancel();
    TDelayedInvoker::Get()->Cancel(Cookie);
    Cookie = NULL;
}

void TPeriodicInvoker::PerformAction()
{
    Action->Do();
    Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TPeriodicInvoker::PerformAction, TPtr(this))
        ->Via(~CancelableInvoker),
        Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
