#pragma once

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestConnection);

////////////////////////////////////////////////////////////////////////////////

TTestConnectionPtr CreateConnection(
    NRpc::IChannelFactoryPtr channelFactory,
    NNodeTrackerClient::TNetworkPreferenceList networkPreferenceList,
    NNodeTrackerClient::TNodeDirectoryPtr nodeDirectory,
    IInvokerPtr invoker,
    INodeMemoryTrackerPtr nodeMemoryTracker);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

namespace NYT::NChaosClient {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TTestChaosResidencyCache);
DECLARE_REFCOUNTED_CLASS(TTestChaosConnection);
DECLARE_REFCOUNTED_CLASS(TTestChaosLeaseWatcherClientCallbackState);
DECLARE_REFCOUNTED_CLASS(TTestChaosNodeService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChaosClient
