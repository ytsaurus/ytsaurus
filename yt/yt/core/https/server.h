#pragma once

#include "public.h"

#include "yt/yt/core/actions/public.h"

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>

namespace NYT::NHttps {

////////////////////////////////////////////////////////////////////////////////

NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller);

NHttp::IServerPtr CreateServer(
    const TServerConfigPtr& config,
    const NConcurrency::IPollerPtr& poller,
    const NConcurrency::IPollerPtr& acceptor,
    const IInvokerPtr& controlInvoker,
    const IMemoryUsageTrackerPtr& memoryTracker = GetNullMemoryUsageTracker());

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttps
