#pragma once

#include "public.h"

#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/client/misc/workload.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobThrottlerType,
    (InBandwidth)
    (OutBandwidth)
    (OutRps)
);

// These throttlers limit total network bandwidth to/from node,
// coordinating network usage of all jobs via RPC calls to yt_node.
// Only method #Throttle is supported.

NConcurrency::IThroughputThrottlerPtr CreateInJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId);

NConcurrency::IThroughputThrottlerPtr CreateOutJobBandwidthThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId);

NConcurrency::IThroughputThrottlerPtr CreateOutJobRpsThrottler(
    const TJobThrottlerConfigPtr& config,
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
