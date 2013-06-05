#include "stdafx.h"
#include "invoker_util.h"
#include "callback.h"

#include <stack>

#include <ytlib/misc/singleton.h>

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

////////////////////////////////////////////////////////////////////////////////

//! Pointer to a per-thread variable used for maintaining the current invoker.
/*!
 *  Examining |CurrentInvoker| could be useful for debugging purposes so we don't
 *  put it into an anonymous namespace to avoid name mangling.
 */
TLS_STATIC IInvokerPtr* CurrentInvoker = nullptr;

namespace {

void InitTls()
{
    if (UNLIKELY(!CurrentInvoker)) {
        CurrentInvoker = new IInvokerPtr();
        *CurrentInvoker = GetSyncInvoker();
    }
}

} // namespace

IInvokerPtr GetCurrentInvoker()
{
    InitTls();
    return *CurrentInvoker;
}

void SetCurrentInvoker(IInvokerPtr invoker)
{
    InitTls();
    *CurrentInvoker = std::move(invoker);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
