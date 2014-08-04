#pragma once

#include "public.h"

#include <core/misc/public.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    static TDispatcher* Get();

    IInvokerPtr GetPoolInvoker();

    DECLARE_SINGLETON_DEFAULT_MIXIN(TDispatcher);

private:
    TDispatcher();

    ~TDispatcher();

    NConcurrency::TThreadPoolPtr ThreadPool;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
