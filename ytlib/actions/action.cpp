#include "action.h"

#include "invoker.h"
#include "action_util.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace {

void ActionViaThunk(IInvoker::TPtr invoker, IAction::TPtr action)
{
    invoker->Invoke(action);
}

} // namespace <anonymous>

IAction::TPtr IAction::Via(IInvoker::TPtr invoker)
{
    return FromMethod(
        ActionViaThunk,
        invoker,
        this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
