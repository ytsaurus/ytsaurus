#pragma once

#include "public.h"

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HttpProxyLogger;
extern const NProfiling::TRegistry HttpProxyProfiler;

extern const NLogging::TLogger HttpStructuredProxyLogger;

DECLARE_REFCOUNTED_CLASS(TFramingAsyncOutputStream);

DECLARE_REFCOUNTED_CLASS(TApiTestingOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
