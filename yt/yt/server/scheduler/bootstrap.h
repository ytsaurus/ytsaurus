#pragma once

#include "public.h"

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/node_tracker_client/public.h>

#include <yt/yt/ytlib/transaction_client/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/action_queue.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NScheduler {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public NServer::IDaemonBootstrap
{
public:
    TBootstrap(
        TSchedulerBootstrapConfigPtr config,
        NYTree::INodePtr configNode,
        NFusion::IServiceLocatorPtr serviceLocator);
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

    void Reconfigure(const TSchedulerConfigPtr& config);

    TFuture<void> Run() final;

private:
    const TSchedulerBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const NFusion::IServiceLocatorPtr ServiceLocator_;
    const NConcurrency::IEnumIndexedFairShareActionQueuePtr<EControlQueue> ControlQueue_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;
    NHttp::IServerPtr HttpServer_;
    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr Client_;
    TSchedulerPtr Scheduler_;
    TControllerAgentTrackerPtr ControllerAgentTracker_;
    mutable THashMap<NObjectClient::TCellTag, NApi::NNative::IClientPtr> RemoteClients_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;

    void DoRun();
    void DoInitialize();
    void DoStart();
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateSchedulerBootstrap(
    TSchedulerBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NScheduler
