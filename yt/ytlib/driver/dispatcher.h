#pragma once

#include "public.h"

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/actions/action_queue.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();

    static TDispatcher* Get();

    void Configure(TDriverConfigPtr config);

    IInvokerPtr GetLightInvoker();
    IInvokerPtr GetHeavyInvoker();

    void Shutdown();

private:
    int HeavyPoolSize;

    /*!
     * This thread is used for light driver commands in #TDriver
     */
    TLazyPtr<TActionQueue> DriverThread;

    /*!
     * This thread pool is used for heavy commands.
     */
    TLazyPtr<TThreadPool> HeavyThreadPool;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

