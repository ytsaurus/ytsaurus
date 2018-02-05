#pragma once

#include "public.h"

#include <yt/server/scheduler/public.h>

#include <yt/server/controller_agent/public.h>

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/hive/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/http/public.h>

#include <yt/core/misc/public.h>

namespace NYT {
namespace NCellScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TCellSchedulerConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TCellSchedulerConfigPtr& GetConfig() const;
    const NApi::INativeClientPtr& GetMasterClient() const;
    const NRpc::IChannelPtr GetLocalRpcChannel() const;
    NNodeTrackerClient::TAddressMap GetLocalAddresses() const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    const NScheduler::TSchedulerPtr& GetScheduler() const;
    const NScheduler::TControllerAgentTrackerPtr& GetControllerAgentTracker() const;
    const NControllerAgent::TControllerAgentPtr& GetControllerAgent() const;
    const NNodeTrackerClient::TNodeDirectoryPtr& GetNodeDirectory() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;
    const TCoreDumperPtr& GetCoreDumper() const;

    NApi::INativeConnectionPtr FindRemoteConnection(NObjectClient::TCellTag cellTag);
    NApi::INativeConnectionPtr GetRemoteConnectionOrThrow(NObjectClient::TCellTag cellTag);

    void Run();

private:
    const TCellSchedulerConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NConcurrency::TFairShareActionQueuePtr ControlQueue_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IChannelPtr LocalRpcChannel_;
    NHttp::IServerPtr HttpServer_;
    NApi::INativeConnectionPtr Connection_;
    NApi::INativeClientPtr Client_;
    NScheduler::TSchedulerPtr Scheduler_;
    NScheduler::TControllerAgentTrackerPtr ControllerAgentTracker_;
    NControllerAgent::TControllerAgentPtr ControllerAgent_;
    NNodeTrackerClient::TNodeDirectoryPtr NodeDirectory_;
    NNodeTrackerClient::TNodeDirectorySynchronizerPtr NodeDirectorySynchronizer_;
    NRpc::TResponseKeeperPtr ResponseKeeper_;
    TCoreDumperPtr CoreDumper_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellScheduler
} // namespace NYT
