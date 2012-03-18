#include "stdafx.h"
#include "periodic_invoker.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/actions/action_util.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPeriodicInvoker::TPeriodicInvoker(IAction::TPtr action, TDuration period)
    : Action(action)
    , Period(period)
    , CancelableContext(New<TCancelableContext>())
    , CancelableInvoker(CancelableContext->CreateInvoker(TSyncInvoker::Get()))
{ }

bool TPeriodicInvoker::IsActive() const
{
    return CancelableContext->IsCanceled();
}

void TPeriodicInvoker::Start()
{
    YASSERT(!IsActive());
    RunAction();
}

void TPeriodicInvoker::Stop()
{
    CancelableContext->Cancel();
    TDelayedInvoker::CancelAndClear(Cookie);
}

void TPeriodicInvoker::RunAction()
{
    Action->Do();
    Cookie = TDelayedInvoker::Submit(
        FromMethod(&TPeriodicInvoker::RunAction, MakeStrong(this))
        ->Via(~CancelableInvoker),
        Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
