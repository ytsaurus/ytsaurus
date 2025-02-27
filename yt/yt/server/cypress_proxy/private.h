#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

enum class EInvokeResult
{
    Executed,
    ForwardToMaster,
};

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(TCypressProxyBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TCypressProxyProgramConfig)

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressProxyLogger, "CypressProxy");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, CypressProxyProfiler, "/cypress_proxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
