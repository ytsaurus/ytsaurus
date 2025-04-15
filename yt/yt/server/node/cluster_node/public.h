#pragma once

#include <yt/yt/server/master/cell_server/public.h>

#include <yt/yt/server/lib/misc/public.h>

#include <yt/yt/client/hydra/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/core/misc/public.h>
#include <yt/yt/core/misc/error.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBootstrapBase)
DECLARE_REFCOUNTED_STRUCT(IBootstrap)
DECLARE_REFCOUNTED_STRUCT(ISlot)
DECLARE_REFCOUNTED_STRUCT(IMasterConnector)

DECLARE_REFCOUNTED_STRUCT(TResourceLimitsConfig)
DECLARE_REFCOUNTED_STRUCT(TResourceLimitsDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TResourceLimitsOverrides)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorConfig)
DECLARE_REFCOUNTED_STRUCT(TMasterConnectorDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChunkReplicaCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TChaosResidencyCacheDynamicConfig)
DECLARE_REFCOUNTED_STRUCT(TTopLevelPortoEnvironmentConfig);
DECLARE_REFCOUNTED_STRUCT(TClusterNodeBootstrapConfig)
DECLARE_REFCOUNTED_STRUCT(TClusterNodeProgramConfig)
DECLARE_REFCOUNTED_STRUCT(TClusterNodeDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfigManager)
DECLARE_REFCOUNTED_STRUCT(TProxyingChunkServiceConfig)
DECLARE_REFCOUNTED_CLASS(TNodeResourceManager)
DECLARE_REFCOUNTED_STRUCT(TMemoryLimit)

using TMasterEpoch = int;

////////////////////////////////////////////////////////////////////////////////

static const THashSet<TErrorCode> HeartbeatRetriableErrors = {
    NHydra::EErrorCode::ReadOnly,
    NCellServer::EErrorCode::MasterCellNotReady,
};

////////////////////////////////////////////////////////////////////////////////

YT_DEFINE_ERROR_ENUM(
    ((UnrecognizedConfigOption)              (2500))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
