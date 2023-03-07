#pragma once

#include <yt/server/lib/misc/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/core/misc/public.h>

namespace NYT::NClusterNode {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap;

DECLARE_REFCOUNTED_CLASS(TResourceLimitsConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManagerConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeConnectionConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeConfig)
DECLARE_REFCOUNTED_CLASS(TClusterNodeDynamicConfig)
DECLARE_REFCOUNTED_CLASS(TBatchingChunkServiceConfig)
DECLARE_REFCOUNTED_CLASS(TDynamicConfigManager)
DECLARE_REFCOUNTED_CLASS(TNodeResourceManager)
DECLARE_REFCOUNTED_CLASS(TMemoryLimit)

using NNodeTrackerClient::TNodeMemoryTracker;
using NNodeTrackerClient::TNodeMemoryTrackerPtr;
using NNodeTrackerClient::TNodeMemoryTrackerGuard;

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EErrorCode,
    ((UnrecognizedConfigOption)              (1400))
    ((FailedToFetchDynamicConfig)            (1401))
    ((DuplicateMatchingDynamicConfigs)       (1402))
    ((UnrecognizedDynamicConfigOption)       (1403))
    ((FailedToApplyDynamicConfig)            (1404))
    ((InvalidDynamicConfig)                  (1405))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClusterNode
