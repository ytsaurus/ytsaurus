#include "stdafx.h"
#include "invoker.h"
#include "invoker_util.h"
#include "callback.h"

#include <ytlib/misc/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

void TSyncInvoker::Invoke(const TClosure& action)
{
    action.Run();
}

IInvokerPtr TSyncInvoker::Get()
{
    return RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
