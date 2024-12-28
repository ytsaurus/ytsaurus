#pragma once

#include "public.h"

#include <yt/yt/server/node/cluster_node/public.h>

#include <yt/yt/server/lib/misc/cluster_throttlers_config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/ytlib/distributed_throttler/public.h>

#include <yt/yt/ytlib/scheduler/cluster_name.h>

#include <yt/yt/core/concurrency/throughput_throttler.h>

namespace NYT::NExecNode {

////////////////////////////////////////////////////////////////////////////////

class IThrottlerManager
    : public virtual TRefCounted
{
public:
    struct TIncomingTrafficUtilization
    {
        // Total (across all exe nodes) incoming rate in bytes per second.
        double Rate = 0;
        // Total (across all exe nodes) incoming rate limit in bytes per second.
        double Limit = 1.0;
        double RateLimitRatioHardThreshold = 1.0;
        double RateLimitRatioSoftThreshold = 1.0;
        // Total (across all exe nodes) pending bytes to be read.
        i64 PendingBytes = 0;
        // Maximum (across all exe nodes) estimated time required to read pending bytes.
        i64 MaxEstimatedTimeToReadPendingBytes = 0;
        i64 MaxEstimatedTimeToReadPendingBytesThreshold = std::numeric_limits<i64>::max();
        // Minimum (across all exe nodes) estimated time required to read pending bytes.
        i64 MinEstimatedTimeToReadPendingBytes = std::numeric_limits<i64>::max();
        i64 MinEstimatedTimeToReadPendingBytesThreshold = std::numeric_limits<i64>::max();
    };

    virtual NConcurrency::IThroughputThrottlerPtr GetOrCreateThrottler(
        EExecNodeThrottlerKind kind,
        EThrottlerTrafficType trafficType,
        std::optional<NScheduler::TClusterName> remoteClusterName) = 0;

    virtual void Reconfigure(NClusterNode::TClusterNodeDynamicConfigPtr dynamicConfig) = 0;

    virtual const NServer::TClusterThrottlersConfigPtr GetClusterThrottlersConfig() const = 0;

    //! Only the leader has incoming traffic utilization collected over all members.
    virtual std::optional<THashMap<NScheduler::TClusterName, TIncomingTrafficUtilization>> GetClusterToIncomingTrafficUtilization(EThrottlerTrafficType trafficType) const = 0;
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
