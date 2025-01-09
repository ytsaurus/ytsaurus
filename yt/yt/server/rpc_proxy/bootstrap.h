#pragma once

#include "public.h"

#include <yt/yt/server/lib/signature/public.h>

#include <yt/yt/server/lib/signature/key_stores/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/client/node_tracker_client/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/tracing/jaeger/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/grpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public NServer::IDaemonBootstrap
{
public:
    TBootstrap(
        TProxyBootstrapConfigPtr config,
        NYTree::INodePtr configNode,
        NFusion::IServiceLocatorPtr serviceLocator);
    ~TBootstrap();

    void Initialize();

    TFuture<void> Run() final;

private:
    const TProxyBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const NFusion::IServiceLocatorPtr ServiceLocator_;

    const NConcurrency::TActionQueuePtr ControlQueue_;
    const NConcurrency::IThreadPoolPtr WorkerPool_;
    const NConcurrency::IPollerPtr HttpPoller_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NBus::IBusServerPtr BusServer_;
    NBus::IBusServerPtr TvmOnlyBusServer_;
    IApiServicePtr ApiService_;
    IApiServicePtr TvmOnlyApiService_;
    NRpc::IServicePtr DiscoveryService_;
    NRpc::IServicePtr ShuffleService_;
    NRpc::IServerPtr RpcServer_;
    NRpc::IServerPtr TvmOnlyRpcServer_;
    NRpc::IServerPtr GrpcServer_;
    NHttp::IServerPtr HttpServer_;

    NApi::NNative::IConnectionPtr Connection_;
    NRpc::IAuthenticatorPtr NativeAuthenticator_;
    NApi::NNative::IClientPtr RootClient_;
    NAuth::IAuthenticationManagerPtr AuthenticationManager_;
    NAuth::IAuthenticationManagerPtr TvmOnlyAuthenticationManager_;
    TProxyHeapUsageProfilerPtr RpcProxyHeapUsageProfiler_;
    IProxyCoordinatorPtr ProxyCoordinator_;
    NTracing::TSamplerPtr TraceSampler_;
    NNodeTrackerClient::TAddressMap LocalAddresses_;
    IDynamicConfigManagerPtr DynamicConfigManager_;
    IBundleDynamicConfigManagerPtr BundleDynamicConfigManager_;
    IAccessCheckerPtr AccessChecker_;

    INodeMemoryTrackerPtr MemoryUsageTracker_;

    NSignature::TCypressKeyReaderPtr CypressKeyReader_;
    NSignature::TSignatureValidatorPtr SignatureValidator_;

    NSignature::TCypressKeyWriterPtr CypressKeyWriter_;
    NSignature::TSignatureGeneratorPtr SignatureGenerator_;
    NSignature::TKeyRotatorPtr SignatureKeyRotator_;

    IQueryCorpusReporterPtr QueryCorpusReporter_;

    void DoInitialize();

    void DoRun();

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);

    void OnBundleDynamicConfigChanged(
        const TBundleProxyDynamicConfigPtr& /*oldConfig*/,
        const TBundleProxyDynamicConfigPtr& newConfig);

    const IInvokerPtr& GetWorkerInvoker() const;
    const IInvokerPtr& GetControlInvoker() const;

    void ReconfigureMemoryLimits(const TProxyMemoryLimitsPtr& memoryLimits);

    void ReconfigureConnection(
        const TProxyDynamicConfigPtr& dynamicConfig,
        const TBundleProxyDynamicConfigPtr& bundleConfig);
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateRpcProxyBootstrap(
    TProxyBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpcProxy
