#pragma once

#include "public.h"

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();

    static TDispatcher* Get();

    IInvokerPtr GetPoolInvoker();

    void Shutdown();

private:
    NConcurrency::TThreadPoolPtr ThreadPool;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
