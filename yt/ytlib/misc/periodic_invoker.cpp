#include "stdafx.h"
#include "periodic_invoker.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/actions/bind.h>
#include <ytlib/logging/log.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger ("PeriodicInvoker");

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

    LOG_TRACE("Instance %p started", this);
    RunAction();
}

void TPeriodicInvoker::Stop()
{
    CancelableContext->Cancel();
    TDelayedInvoker::CancelAndClear(Cookie);

    LOG_TRACE("Instance %p stopped", this);
}

void TPeriodicInvoker::RunAction()
{
    LOG_TRACE("Action started in instance %p", this);
    Action.Run();
    LOG_TRACE("Action completed in instance %p", this);

    Cookie = TDelayedInvoker::Submit(
        BIND(&TPeriodicInvoker::RunAction, MakeStrong(this)).Via(CancelableInvoker),
        Period);
    LOG_TRACE("New cookie for instance %p is %p", this, ~Cookie);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
