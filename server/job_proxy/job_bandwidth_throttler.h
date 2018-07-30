#pragma once

#include <yt/server/exec_agent/supervisor_service_proxy.h>

#include <yt/client/misc/workload.h>

#include <yt/core/concurrency/public.h>

namespace NYT {
namespace NJobProxy {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(EJobBandwidthDirection,
    (In)
    (Out)
);

// These throttlers limit total network bandwidth to/from node,
// coordinating network usage of all jobs via RPC calls to yt_node.
// Only method #Throttle is supported.

NConcurrency::IThroughputThrottlerPtr CreateInJobBandwidthThrottler(
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId,
    TDuration timeout);

NConcurrency::IThroughputThrottlerPtr CreateOutJobBandwidthThrottler(
    const NRpc::IChannelPtr& channel,
    const TWorkloadDescriptor& descriptor,
    NJobTrackerClient::TJobId jobId,
    TDuration timeout);

////////////////////////////////////////////////////////////////////////////////

} // namespace NJobProxy
} // namespace NYT
