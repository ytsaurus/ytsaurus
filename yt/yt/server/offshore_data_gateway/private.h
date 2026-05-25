#pragma once

#include <yt/yt/server/lib/alert_manager/helpers.h>

#include <yt/yt/core/logging/log.h>

#include <yt/yt/library/profiling/sensor.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_GLOBAL(const NLogging::TLogger, OffshoreDataGatewayLogger, "OffshoreDataGateway");

YT_DEFINE_GLOBAL(const NProfiling::TProfiler, OffshoreDataGatewayProfiler, "/offshore_data_gateway");

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TOffshoreDataGatewayBootstrapConfig)
DECLARE_REFCOUNTED_CLASS(TOffshoreDataGatewayProgramConfig)
DECLARE_REFCOUNTED_CLASS(TOffshoreDataGatewayDynamicConfig)

DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrap)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
