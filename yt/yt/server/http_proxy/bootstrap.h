#pragma once

#include "public.h"

#include <yt/yt/server/http_proxy/clickhouse/public.h>

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/misc/bootstrap.h>

#include <yt/yt/server/lib/signature/public.h>

#include <yt/yt/ytlib/api/public.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/client/driver/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/library/containers/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/coredumper/public.h>

#include <yt/yt/library/disk_manager/public.h>

#include <yt/yt/core/bus/public.h>

#include <yt/yt/core/actions/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/misc/public.h>

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/core/http/public.h>

#include <yt/yt/core/http/http.h>

#include <yt/yt/core/https/public.h>

#include <yt/yt/core/ytree/public.h>

#include <library/cpp/yt/memory/atomic_intrusive_ptr.h>

namespace NYT::NHttpProxy {

////////////////////////////////////////////////////////////////////////////////

class TBootstrap
    : public NHttp::IHttpHandler
    , public NServer::IDaemonBootstrap
{
public:
    TBootstrap(
        TProxyBootstrapConfigPtr config,
        NYTree::INodePtr configNode,
        NFusion::IServiceLocatorPtr serviceLocator);
    ~TBootstrap();

    TFuture<void> Run() final;

    const IInvokerPtr& GetControlInvoker() const;
    const TProxyBootstrapConfigPtr& GetConfig() const;
    TProxyDynamicConfigPtr GetDynamicConfig() const;
    const NApi::IClientPtr& GetRootClient() const;
    const NApi::NNative::IConnectionPtr& GetNativeConnection() const;
    const NDriver::IDriverPtr& GetDriverV3() const;
    const NDriver::IDriverPtr& GetDriverV4() const;
    const TCoordinatorPtr& GetCoordinator() const;
    const IAccessCheckerPtr& GetAccessChecker() const;
    const TCompositeHttpAuthenticatorPtr& GetHttpAuthenticator() const;
    const NAuth::IAuthenticationManagerPtr& GetAuthenticationManager() const;
    const NAuth::ITokenAuthenticatorPtr& GetTokenAuthenticator() const;
    const NAuth::ICookieAuthenticatorPtr& GetCookieAuthenticator() const;
    const IDynamicConfigManagerPtr& GetDynamicConfigManager() const;
    const NConcurrency::IPollerPtr& GetPoller() const;
    const INodeMemoryTrackerPtr& GetMemoryUsageTracker() const;
    const TApiPtr& GetApi() const;

    bool IsChytApiServerAddress(const NNet::TNetworkAddress& address) const;

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) final;

private:
    const TProxyBootstrapConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const NFusion::IServiceLocatorPtr ServiceLocator_;

    const TInstant StartTime_ = TInstant::Now();

    TAtomicIntrusivePtr<TProxyDynamicConfig> DynamicConfig_ = TAtomicIntrusivePtr(New<TProxyDynamicConfig>());

    const NConcurrency::TActionQueuePtr Control_;
    const NConcurrency::IPollerPtr Poller_;
    const NConcurrency::IPollerPtr Acceptor_;

    NMonitoring::IMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr MonitoringServer_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::IClientPtr RootClient_;
    NDriver::IDriverPtr DriverV3_;
    NDriver::IDriverPtr DriverV4_;

    NAuth::IAuthenticationManagerPtr AuthenticationManager_;
    NAuth::IAuthenticationManagerPtr TvmOnlyAuthenticationManager_;
    TCompositeHttpAuthenticatorPtr HttpAuthenticator_;

    NHttp::IHttpHandlerPtr CypressCookieLoginHandler_;

    IDynamicConfigManagerPtr DynamicConfigManager_;

    TProxyHeapUsageProfilerPtr HttpProxyHeapUsageProfiler_;

    NBus::IBusServerPtr BusServer_;
    NRpc::IServerPtr RpcServer_;

    NHttp::IServerPtr ApiHttpServer_;
    NHttp::IServerPtr ApiHttpsServer_;
    NHttp::IServerPtr TvmOnlyApiHttpServer_;
    NHttp::IServerPtr TvmOnlyApiHttpsServer_;
    NHttp::IServerPtr ChytApiHttpServer_;
    NHttp::IServerPtr ChytApiHttpsServer_;

    TApiPtr Api_;

    NClickHouse::TClickHouseHandlerPtr ClickHouseHandler_;

    TCoordinatorPtr Coordinator_;
    THostsHandlerPtr HostsHandler_;
    TClusterConnectionHandlerPtr ClusterConnectionHandler_;
    TPingHandlerPtr PingHandler_;
    TDiscoverVersionsHandlerPtr DiscoverVersionsHandler_;

    NProfiling::TSolomonProxyPtr SolomonProxy_;

    IAccessCheckerPtr AccessChecker_;

    INodeMemoryTrackerPtr MemoryUsageTracker_;

    NSignature::ISignatureGeneratorPtr SignatureGenerator_;
    NSignature::ISignatureValidatorPtr SignatureValidator_;
    NSignature::TKeyRotatorPtr SignatureKeyRotator_;

    void DoRun();
    void DoInitialize();
    void DoStart();

    void RegisterRoutes(const NHttp::IServerPtr& server);
    NHttp::IHttpHandlerPtr AllowCors(NHttp::IHttpHandlerPtr nextHandler) const;

    void SetupClients();

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);

    void ReconfigureMemoryLimits(const TProxyMemoryLimitsConfigPtr& memoryLimits);
};

DEFINE_REFCOUNTED_TYPE(TBootstrap)

////////////////////////////////////////////////////////////////////////////////

TBootstrapPtr CreateHttpProxyBootstrap(
    TProxyBootstrapConfigPtr config,
    NYTree::INodePtr configNode,
    NFusion::IServiceLocatorPtr serviceLocator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
