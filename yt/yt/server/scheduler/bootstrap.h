#pragma once

#include "public.h"

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/http/public.h>

#include <yt/core/rpc/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TSchedulerBootstrapConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TSchedulerBootstrapConfigPtr& GetConfig() const;
    const NApi::NNative::IClientPtr& GetMasterClient() const;
    const NApi::NNative::IClientPtr& GetRemoteMasterClient(NObjectClient::TCellTag tag) const;
    NNodeTrackerClient::TAddressMap GetLocalAddresses() const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue) const;
    const TSchedulerPtr& GetScheduler() const;
    const TControllerAgentTrackerPtr& GetControllerAgentTracker() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;
    const ICoreDumperPtr& GetCoreDumper() const;

    void Run();

private:
    const TSchedulerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NConcurrency::TFairShareActionQueuePtr ControlQueue_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;
    TSchedulerPtr Scheduler_;
    TControllerAgentTrackerPtr ControllerAgentTracker_;
    NRpc::TResponseKeeperPtr ResponseKeeper_;
    ICoreDumperPtr CoreDumper_;
    mutable THashMap<NObjectClient::TCellTag, NApi::NNative::IClientPtr> RemoteClients_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
