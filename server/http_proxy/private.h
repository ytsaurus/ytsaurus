#pragma once

#include <yt/core/logging/log.h>

#include <yt/core/profiling/profiler.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

extern const NLogging::TLogger HttpProxyLogger;
extern const NProfiling::TProfiler HttpProxyProfiler;

extern const NLogging::TLogger HttpStructuredProxyLogger;

extern const TString ClickHouseUserName;

DECLARE_REFCOUNTED_CLASS(TFramingAsyncOutputStream);

DECLARE_REFCOUNTED_CLASS(TApiTestingOptions);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
