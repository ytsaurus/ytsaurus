#pragma once

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/misc/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

struct IBootstrap;
struct IBootstrapBase;

DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TResourceLimitsDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorConfig)
DECLARE_REFCOUNTED_CLASS(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TBatchingChunkServiceConfig)
DECLARE_REFCOUNTED_CLASS(TNodeResourceManager)
DECLARE_REFCOUNTED_CLASS(TMemoryLimit)

using TMasterEpoch = int;

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((UnrecognizedConfigOption)              (2500))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
