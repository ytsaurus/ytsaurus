#pragma once

#include "public.h"

#include <ytlib/actions/action_queue.h>

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
    TThreadPoolPtr ThreadPool;

};

////////////////////////////////////////////////////////////////////////////////
            
} // namespace NRpc
} // namespace NYT
