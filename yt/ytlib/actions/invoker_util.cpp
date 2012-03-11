#include "stdafx.h"
#include "invoker_util.h"

#include <ytlib/misc/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSyncInvoker::Invoke(IAction::TPtr action)
{
    action->Do();
}

IInvoker* TSyncInvoker::Get()
{
    return ~RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
