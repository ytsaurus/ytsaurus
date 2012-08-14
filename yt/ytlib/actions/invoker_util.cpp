#include "stdafx.h"
#include "invoker.h"
#include "invoker_util.h"
#include "callback.h"

#include <ytlib/misc/singleton.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual bool Invoke(const TClosure& action)
    {
        action.Run();
        return true;
    }
};

IInvokerPtr GetSyncInvoker()
{
    return RefCountedSingleton<TSyncInvoker>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
