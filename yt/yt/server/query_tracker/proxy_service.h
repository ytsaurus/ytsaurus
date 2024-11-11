#pragma once

#include "private.h"

#include <yt/yt/server/lib/state_checker/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT::NQueryTracker {

////////////////////////////////////////////////////////////////////////////////

NRpc::IServicePtr CreateProxyService(
    IInvokerPtr proxyInvoker,
    TQueryTrackerProxyPtr proxy,
    NComponentStateChecker::IComponentStateCheckerPtr ComponentStateChecker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryTracker
