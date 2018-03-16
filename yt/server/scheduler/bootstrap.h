#pragma once

#include "public.h"

#include <yt/ytlib/api/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/ytlib/node_tracker_client/public.h>

#include <yt/ytlib/transaction_client/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/concurrency/action_queue.h>

#include <yt/core/http/public.h>

#include <yt/core/rpc/public.h>

namespace NYT {
namespace NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TSchedulerBootstrapConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TSchedulerBootstrapConfigPtr& GetConfig() const;
    const NApi::INativeClientPtr& GetMasterClient() const;
    NNodeTrackerClient::TAddressMap GetLocalAddresses() const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue = EControlQueue::Default) const;
    const TSchedulerPtr& GetScheduler() const;
    const TControllerAgentTrackerPtr& GetControllerAgentTracker() const;
    const NRpc::TResponseKeeperPtr& GetResponseKeeper() const;
    const ICoreDumperPtr& GetCoreDumper() const;

    void Run();

    // XXX(babenko): finish separation
//private:
    const TSchedulerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
    NConcurrency::TFairShareActionQueuePtr ControlQueue_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NApi::INativeConnectionPtr Connection_;
    NApi::INativeClientPtr Client_;
    TSchedulerPtr Scheduler_;
    TControllerAgentTrackerPtr ControllerAgentTracker_;
    NRpc::TResponseKeeperPtr ResponseKeeper_;
    ICoreDumperPtr CoreDumper_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NScheduler
} // namespace NYT
