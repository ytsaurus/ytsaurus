#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <ytlib/misc/singleton.h>

#include <ytlib/concurrency/fiber.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TSyncInvoker
    : public IInvoker
{
public:
    virtual bool Invoke(const TClosure& action) override
    {
        action.Run();
        return true;
    }
};

IInvokerPtr GetSyncInvoker()
{
    return RefCountedSingleton<TSyncInvoker>();
}

IInvokerPtr GetCurrentInvoker()
{
    return TFiber::GetCurrent()->GetCurrentInvoker();
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    TFiber::GetCurrent()->SetCurrentInvoker(std::move(invoker));
}

////////////////////////////////////////////////////////////////////////////////

TCurrentInvokerGuard::TCurrentInvokerGuard(IInvokerPtr newInvoker)
{
    OldInvoker = GetCurrentInvoker();
    SetCurrentInvoker(std::move(newInvoker));
}

TCurrentInvokerGuard::~TCurrentInvokerGuard()
{
    SetCurrentInvoker(std::move(OldInvoker));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
