#pragma once

#include "public.h"

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TSchedulerBootstrapConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TSchedulerBootstrapConfigPtr& GetConfig() const;
    const NApi::NNative::IClientPtr& GetClient() const;
    const NApi::NNative::IClientPtr& GetRemoteClient(NObjectClient::TCellTag tag) const;
    NNodeTrackerClient::TAddressMap GetLocalAddresses() const;
    NNodeTrackerClient::TNetworkPreferenceList GetLocalNetworks() const;
    IInvokerPtr GetControlInvoker(EControlQueue queue) const;
    const TSchedulerPtr& GetScheduler() const;
    const TControllerAgentTrackerPtr& GetControllerAgentTracker() const;
    const NRpc::IAuthenticatorPtr& GetNativeAuthenticator() const;

    void OnDynamicConfigChanged(const TSchedulerConfigPtr& config);

    void Run();

private:
    const TSchedulerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NConcurrency::IEnumIndexedFairShareActionQueuePtr<EControlQueue> ControlQueue_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;
    TSchedulerPtr Scheduler_;
    TControllerAgentTrackerPtr ControllerAgentTracker_;
    NCoreDump::ICoreDumperPtr CoreDumper_;
    mutable THashMap<NObjectClient::TCellTag, NApi::NNative::IClientPtr> RemoteClients_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
