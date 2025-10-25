#pragma once

#include "public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/lazy_ptr.h>

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NPipeIO {

////////////////////////////////////////////////////////////////////////////////

class TPipeIODispatcher
{
public:
    static TPipeIODispatcher* Get();
    ~TPipeIODispatcher();

    void Configure(const TPipeIODispatcherConfigPtr& config);

    IInvokerPtr GetInvoker();
    NConcurrency::IPollerPtr GetPoller();

private:
    TPipeIODispatcher();

    Y_DECLARE_SINGLETON_FRIEND()

    TLazyIntrusivePtr<NConcurrency::IThreadPoolPoller> Poller_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NPipeIO
