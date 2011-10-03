#include "periodic_invoker.h"

#include "../actions/action_util.h"

namespace NYT {

TPeriodicInvoker::TPeriodicInvoker(IAction::TPtr action, TDuration period)
    : Action(action)
    , Period(period)
{ }

TPeriodicInvoker::~TPeriodicInvoker()
{
    if (IsActive()) {
        Stop();
    }
}

bool TPeriodicInvoker::IsActive() const
{
    return ~Cookie != NULL;
}

void TPeriodicInvoker::Start()
{
    PerformAction();
}

void TPeriodicInvoker::Stop()
{
    YASSERT(IsActive());
    TDelayedInvoker::Get()->Cancel(Cookie);
    Cookie = NULL;
}

void TPeriodicInvoker::PerformAction()
{
    Action->Do();
    Cookie = TDelayedInvoker::Get()->Submit(
        FromMethod(&TPeriodicInvoker::PerformAction, TPtr(this)),
        Period);
}

} // namespace NYT
