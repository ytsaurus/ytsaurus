#pragma once

#include "public.h"

#include <yt/server/rpc_proxy/public.h>

#include <yt/ytlib/auth/public.h>

#include <yt/ytlib/api/native/public.h>

#include <yt/ytlib/monitoring/public.h>

#include <yt/core/bus/public.h>

#include <yt/core/actions/public.h>

#include <yt/core/concurrency/public.h>

#include <yt/core/misc/public.h>

#include <yt/core/rpc/public.h>

#include <yt/core/rpc/grpc/public.h>

#include <yt/core/http/public.h>

#include <yt/core/ytree/public.h>

namespace NYT {
namespace NCellProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
{
public:
    TBootstrap(TCellProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    const TCellProxyConfigPtr& GetConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const IInvokerPtr& GetWorkerInvoker() const;
    const NApi::NNative::IConnectionPtr& GetNativeConnection() const;
    const NApi::NNative::IClientPtr& GetNativeClient() const;
    const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const;
    const NRpcProxy::IProxyCoordinatorPtr& GetProxyCoordinator() const;
    const NNodeTrackerClient::TAddressMap& GetLocalAddresses() const;

    void Run();

private:
    const TCellProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::TThreadPoolPtr WorkerPool_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    std::unique_ptr<NLFAlloc::TLFAllocProfiler> LFAllocProfiler_;
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

    void DoRun();
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NCellProxy
} // namespace NYT
