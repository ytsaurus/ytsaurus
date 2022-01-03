#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

inline const NLogging::TLogger HttpProxyLogger("HttpProxy");
inline const NProfiling::TProfiler HttpProxyProfiler("/http_proxy");

inline const NLogging::TLogger HttpStructuredProxyLogger("HttpStructuredProxy");

DECLARE_REFCOUNTED_CLASS(TFramingAsyncOutputStream)

DECLARE_REFCOUNTED_CLASS(TApiTestingOptions)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
