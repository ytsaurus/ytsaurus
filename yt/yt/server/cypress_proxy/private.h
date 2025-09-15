#pragma once

#include "public.h"

#include <yt/yt/library/profiling/sensor.h>

#include <yt/yt/core/logging/log.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(ISequoiaAttributeFetcher)

struct TRequestExecutedPayload
{ };

struct TForwardToMasterPayload
{
    std::optional<NYson::TYsonString> EffectiveAcl;
};

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, CypressProxyLogger, "CypressProxy");
YT_DEFINE_GLOBAL(const NProfiling::TProfiler, CypressProxyProfiler, "/cypress_proxy");

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
