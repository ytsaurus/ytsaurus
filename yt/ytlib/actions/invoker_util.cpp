#include "stdafx.h"
#include "invoker_util.h"
#include "action.h"
#include "action_util.h"

#include <ytlib/misc/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSyncInvoker::Invoke(IAction::TPtr action)
{
    action->Do();
}

IInvoker::TPtr TSyncInvoker::Get()
{
    return RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

TCancelableInvoker::TCancelableInvoker(IInvoker::TPtr underlyingInvoker)
    : Canceled(false)
    , UnderlyingInvoker(underlyingInvoker)
{
    YASSERT(underlyingInvoker);
}

void TCancelableInvoker::Invoke(IAction::TPtr action)
{
    YASSERT(action);

    if (Canceled)
        return;
    UnderlyingInvoker->Invoke(FromMethod(
        &TCancelableInvoker::ActionThunk,
        TPtr(this),
        action));
}

void TCancelableInvoker::ActionThunk(IAction::TPtr action)
{
    if (Canceled)
        return;
    action->Do();
}

void TCancelableInvoker::Cancel()
{
    Canceled = true;
}

bool TCancelableInvoker::IsCanceled() const
{
    return Canceled;
}

////////////////////////////////////////////////////////////////////////////////

}
