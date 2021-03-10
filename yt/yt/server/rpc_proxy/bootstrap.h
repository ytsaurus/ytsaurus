#pragma once

#include "public.h"

#include <yt/yt/server/rpc_proxy/public.h>

#include <yt/yt/ytlib/auth/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/monitoring/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/rpc/grpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TProxyConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetWorkerInvoker() const;
    const NApi::NNative::IConnectionPtr& GetNativeConnection() const;
    const NApi::NNative::IClientPtr& GetNativeClient() const;
    const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const;
    const NRpcProxy::IProxyCoordinatorPtr& GetProxyCoordinator() const;
    const NNodeTrackerClient::TAddressMap& GetLocalAddresses() const;
    const IAccessCheckerPtr& GetAccessChecker() const;

    void Run();

private:
    const TProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::TThreadPoolPtr WorkerPool_;
    const NConcurrency::IPollerPtr HttpPoller_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NRpc::IServicePtr ApiService_;
    NRpc::IServicePtr DiscoveryService_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IServerPtr GrpcServer_;
    NHttp::IServerPtr HttpServer_;
    ICoreDumperPtr CoreDumper_;

    NApi::NNative::IConnectionPtr NativeConnection_;
    NApi::NNative::IClientPtr NativeClient_;
    NAuth::TAuthenticationManagerPtr AuthenticationManager_;
    NRpcProxy::IProxyCoordinatorPtr ProxyCoordinator_;
    NNodeTrackerClient::TAddressMap LocalAddresses_;
    IAccessCheckerPtr AccessChecker_;

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
