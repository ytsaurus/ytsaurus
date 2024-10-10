#pragma once

#include "public.h"

#include <yt/yt/server/http_proxy/clickhouse/public.h>

#include <yt/yt/server/lib/chunk_pools/public.h>

#include <yt/yt/server/lib/misc/disk_change_checker.h>

#include <yt/yt/server/lib/zookeeper_proxy/bootstrap_proxy.h>

#include <yt/yt/ytlib/api/public.h>
#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/misc/public.h>

#include <yt/yt/client/driver/public.h>

#include <yt/yt/library/auth_server/public.h>

#include <yt/yt/library/monitoring/public.h>

#include <yt/yt/library/containers/public.h>
#include <yt/yt/library/containers/disk_manager/public.h>

#include <yt/yt/library/profiling/solomon/public.h>

#include <yt/yt/library/coredumper/public.h>

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
{
public:
    TBootstrap(TProxyConfigPtr config, NYTree::INodePtr configNode);
    ~TBootstrap();

    void Run();

    const IInvokerPtr& GetControlInvoker() const;

    const TProxyConfigPtr& GetConfig() const;
    TProxyDynamicConfigPtr GetDynamicConfig() const;
    const NApi::NNative::IClientPtr& GetRootClient() const;
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
    const TApiPtr& GetApi() const;

    bool IsChytApiServerAddress(const NNet::TNetworkAddress& address) const;

    void HandleRequest(
        const NHttp::IRequestPtr& req,
        const NHttp::IResponseWriterPtr& rsp) override;

private:
    const TProxyConfigPtr Config_;
    const NYTree::INodePtr ConfigNode_;
    const TInstant StartTime_ = TInstant::Now();

    TAtomicIntrusivePtr<TProxyDynamicConfig> DynamicConfig_ = TAtomicIntrusivePtr(New<TProxyDynamicConfig>());

    NConcurrency::TActionQueuePtr Control_;
    NConcurrency::IPollerPtr Poller_;
    NConcurrency::IPollerPtr Acceptor_;

    NMonitoring::TMonitoringManagerPtr MonitoringManager_;
    NHttp::IServerPtr MonitoringServer_;

    NApi::NNative::IConnectionPtr Connection_;
    NApi::NNative::IClientPtr RootClient_;
    NDriver::IDriverPtr DriverV3_;
    NDriver::IDriverPtr DriverV4_;

    NAuth::IAuthenticationManagerPtr AuthenticationManager_;
    NAuth::IAuthenticationManagerPtr TvmOnlyAuthenticationManager_;
    TCompositeHttpAuthenticatorPtr HttpAuthenticator_;

    NHttp::IHttpHandlerPtr CypressCookieLoginHandler_;

    IDynamicConfigManagerPtr DynamicConfigManager_;

    THttpProxyHeapUsageProfilerPtr HttpProxyHeapUsageProfiler_;

    NRpc::IServerPtr RpcServer_;

    NHttp::IServerPtr ApiHttpServer_;
    NHttp::IServerPtr ApiHttpsServer_;
    NHttp::IServerPtr TvmOnlyApiHttpServer_;
    NHttp::IServerPtr TvmOnlyApiHttpsServer_;
    NHttp::IServerPtr ChytApiHttpServer_;
    NHttp::IServerPtr ChytApiHttpsServer_;

    TApiPtr Api_;

    NClickHouse::TClickHouseHandlerPtr ClickHouseHandler_;

    std::unique_ptr<NZookeeperProxy::IBootstrapProxy> ZookeeperBootstrapProxy_;
    std::unique_ptr<NZookeeperProxy::IBootstrap> ZookeeperBootstrap_;

    TCoordinatorPtr Coordinator_;
    THostsHandlerPtr HostsHandler_;
    TClusterConnectionHandlerPtr ClusterConnectionHandler_;
    TPingHandlerPtr PingHandler_;
    TDiscoverVersionsHandlerPtr DiscoverVersionsHandler_;

    NProfiling::TSolomonProxyPtr SolomonProxy_;

    IAccessCheckerPtr AccessChecker_;

    NCoreDump::ICoreDumperPtr CoreDumper_;

    NContainers::IDiskManagerProxyPtr DiskManagerProxy_;
    NContainers::TDiskInfoProviderPtr DiskInfoProvider_;
    TDiskChangeCheckerPtr DiskChangeChecker_;

    INodeMemoryTrackerPtr MemoryUsageTracker_;

    void RegisterRoutes(const NHttp::IServerPtr& server);
    NHttp::IHttpHandlerPtr AllowCors(NHttp::IHttpHandlerPtr nextHandler) const;

    void SetupClients();

    void OnDynamicConfigChanged(
        const TProxyDynamicConfigPtr& /*oldConfig*/,
        const TProxyDynamicConfigPtr& newConfig);

    void ReconfigureMemoryLimits(const TProxyMemoryLimitsConfigPtr& memoryLimits);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
