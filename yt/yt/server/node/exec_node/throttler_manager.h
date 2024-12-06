#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/misc/cluster_throttlers_config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class IThrottlerManager
    : public virtual TRefCounted
{
public:
    virtual NConcurrency::IThroughputThrottlerPtr GetOrCreateThrottler(
        EExecNodeThrottlerKind kind,
        EExecNodeThrottlerTrafficType traffic,
        std::optional<NScheduler::TClusterName> remoteClusterName) = 0;

    virtual void Reconfigure(NClusterNode::TClusterNodeDynamicConfigPtr dynamicConfig) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThrottlerManager)

struct TThrottlerManagerOptions
{
    NDiscoveryClient::TGroupId GroupId;
    NDiscoveryClient::TMemberId MemberId;
    std::string LocalAddress;
    NLogging::TLogger Logger;
    NProfiling::TProfiler Profiler;
};

IThrottlerManagerPtr CreateThrottlerManager(
    NClusterNode::IBootstrap* bootstrap,
    TThrottlerManagerOptions options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NExecNode
