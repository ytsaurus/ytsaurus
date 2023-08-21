#pragma once

#include "public.h"

#include <yt/yt/server/lib/exec_node/supervisor_service_proxy.h>

#include <yt/yt/client/misc/workload.h>

#include <yt/yt/core/concurrency/public.h>

namespace NYT::NJobProxy {

////////////////////////////////////////////////////////////////////////////////

// These throttlers limit total network bandwidth to/from node,
// coordinating network usage of all jobs via RPC calls to yt_node.
// Only method #Throttle is supported.

NConcurrency::IThroughputThrottlerPtr CreateInJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId,
    const NLogging::TLogger& logger);

NConcurrency::IThroughputThrottlerPtr CreateOutJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId,
    const NLogging::TLogger& logger);

NConcurrency::IThroughputThrottlerPtr CreateOutJobRpsThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId,
    const NLogging::TLogger& logger);

NConcurrency::IThroughputThrottlerPtr CreateUserJobContainerCreationThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJobProxy
