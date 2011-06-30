#include "invoker.h"
#include "action.h"
#include "action_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSyncInvoker::Invoke(IAction::TPtr action)
{
    action->Do();
}

TSyncInvoker* TSyncInvoker::Get()
{
    return ~RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

TCancelableInvoker::TCancelableInvoker()
    : Canceled(false)
{}

void TCancelableInvoker::Invoke(IAction::TPtr action)
{
    if (!Canceled) {
        action->Do();
    }
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
