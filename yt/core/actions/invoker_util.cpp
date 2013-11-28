#include "stdafx.h"
#include "invoker_util.h"

#include <stack>

#include <core/misc/singleton.h>

#include <core/concurrency/fiber.h>

namespace NYT {

using namespace NConcurrency;

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

    virtual NConcurrency::TThreadId GetThreadId() const override
    {
        return NConcurrency::InvalidThreadId;
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
