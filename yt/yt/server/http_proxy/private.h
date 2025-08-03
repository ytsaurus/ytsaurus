#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

constexpr auto HttpProxyUserAllocationTagKey = "user";
constexpr auto HttpProxyRequestIdAllocationTagKey = "request_id";
constexpr auto HttpProxyCommandAllocationTagKey = "command";

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, HttpProxyLogger, "HttpProxy");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, HttpProxyProfiler, "/http_proxy");

extern const NLogging::TLogger HttpStructuredProxyLogger;

DECLARE_REFCOUNTED_CLASS(TFramingAsyncOutputStream)

DECLARE_REFCOUNTED_STRUCT(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
