#pragma once

#include "public.h"

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/lazy_ptr.h>

namespace NYT::NPipes {

////////////////////////////////////////////////////////////////////////////////

class TIODispatcher
{
public:
    ~TIODispatcher();

    static TIODispatcher* Get();

    static void StaticShutdown();

    void Shutdown();

    IInvokerPtr GetInvoker();

    NConcurrency::IPollerPtr GetPoller();

private:
    TIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND();

    TLazyIntrusivePtr<NConcurrency::IPoller> Poller_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipes
