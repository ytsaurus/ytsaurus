#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto HttpProxyUserAllocationTag = "user";
constexpr auto HttpProxyRequestIdAllocationTag = "request_id";
constexpr auto HttpProxyCommandAllocationTag = "command";

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger HttpProxyLogger("HttpProxy");
inline const NProfiling::TProfiler HttpProxyProfiler("/http_proxy");

extern const NLogging::TLogger HttpStructuredProxyLogger;

DECLARE_REFCOUNTED_CLASS(TFramingAsyncOutputStream)

DECLARE_REFCOUNTED_CLASS(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
