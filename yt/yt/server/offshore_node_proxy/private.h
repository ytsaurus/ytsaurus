#pragma once

#include <yt/yt/server/lib/alert_manager/helpers.h>

#include <yt/yt/ytlib/queue_client/public.h>

#include <yt/yt/client/queue_client/common.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, OffshoreNodeProxyLogger, "OffshoreNodeProxy");

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, OffshoreNodeProxyProfiler, "/offshore_node_proxy");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOffshoreNodeProxyBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TOffshoreNodeProxyProgramConfig)
DECLARE_REFCOUNTED_CLASS(TOffshoreNodeProxyDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
