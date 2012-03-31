#include "stdafx.h"
#include "periodic_invoker.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/actions/bind.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

TPeriodicInvoker::TPeriodicInvoker(TClosure action, TDuration period)
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
    Action.Run();
    Cookie = TDelayedInvoker::Submit(
        BIND(&TPeriodicInvoker::RunAction, MakeWeak(this))
        .Via(~CancelableInvoker),
        Period);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
