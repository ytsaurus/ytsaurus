#pragma once

#include "public.h"

#include <yt/yt/server/lib/rpc_proxy/bootstrap.h>

#include <yt/yt/ytlib/auth/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/monitoring/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/atomic_object.h>
#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/grpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/public.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public IBootstrap
{
public:
    TBootstrap(TProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    // IBootstrap implementation.
    virtual const IInvokerPtr& GetWorkerInvoker() const override;
    virtual const NRpc::IAuthenticatorPtr& GetRpcAuthenticator() const override;
    virtual TApiServiceConfigPtr GetConfigApiService() const override;
    virtual NAuth::TAuthenticationManagerConfigPtr GetConfigAuthenticationManager() const override;
    virtual TApiServiceDynamicConfigPtr GetDynamicConfigApiService() const override;
    virtual const NTracing::TSamplerPtr& GetTraceSampler() const override;
    virtual const IProxyCoordinatorPtr& GetProxyCoordinator() const override;
    virtual const IAccessCheckerPtr& GetAccessChecker() const override;
    virtual const NApi::NNative::IConnectionPtr& GetNativeConnection() const override;
    virtual const NApi::NNative::IClientPtr& GetNativeClient() const override;

    const TProxyConfigPtr& GetConfig() const;
    TProxyDynamicConfigPtr GetDynamicConfig() const;
    const IInvokerPtr& GetControlInvoker() const;
    const NNodeTrackerClient::TAddressMap& GetLocalAddresses() const;
    const IDynamicConfigManagerPtr& GetDynamicConfigManager() const;

    void Run();

private:
    const TProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;

    TAtomicObject<TProxyDynamicConfigPtr> DynamicConfig_ = New<TProxyDynamicConfig>();

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
    NTracing::TSamplerPtr TraceSampler_;
    NNodeTrackerClient::TAddressMap LocalAddresses_;
    IDynamicConfigManagerPtr DynamicConfigManager_;
    IAccessCheckerPtr AccessChecker_;

    void DoRun();

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
