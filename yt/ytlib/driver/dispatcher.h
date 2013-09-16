#pragma once

#include "public.h"

#include <core/misc/lazy_ptr.h>

#include <core/concurrency/action_queue.h>

namespace NYT {
namespace NDriver {

////////////////////////////////////////////////////////////////////////////////

class TDispatcher
{
public:
    TDispatcher();

    static TDispatcher* Get();

    void Configure(int heavyPoolSize);

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

