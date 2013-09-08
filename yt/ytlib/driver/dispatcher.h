#pragma once

#include "public.h"

#include <ytlib/misc/lazy_ptr.h>

#include <ytlib/concurrency/action_queue.h>

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
     * This thread is used by TDriver for light commands.
     */
    TLazyIntrusivePtr<NConcurrency::TActionQueue> DriverThread;

    /*!
     * This thread pool is used by TDriver for heavy commands.
     */
    TLazyIntrusivePtr<NConcurrency::TThreadPool> HeavyThreadPool;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NDriver
} // namespace NYT

