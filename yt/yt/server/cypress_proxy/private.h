#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TCypressProxyConfig)

struct IBootstrap;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressProxyLogger, "CypressProxy");
inline const NProfiling::TProfiler CypressProxyProfiler("/cypress_proxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
