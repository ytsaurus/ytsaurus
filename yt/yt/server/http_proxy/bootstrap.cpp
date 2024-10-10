#include "bootstrap.h"

#include "access_checker.h"
#include "config.h"
#include "coordinator.h"
#include "dynamic_config_manager.h"
#include "api.h"
#include "http_authenticator.h"
#include "private.h"
#include "zookeeper_bootstrap_proxy.h"
#include "solomon_proxy.h"

#include <yt/yt/server/http_proxy/clickhouse/handler.h>
#include <yt/yt/server/http_proxy/profilers.h>

#include <yt/yt/server/lib/admin/admin_service.h>

#include <yt/yt/server/lib/misc/disk_change_checker.h>

#include <yt/yt/library/coredumper/coredumper.h>

#include <yt/yt/server/lib/zookeeper_proxy/bootstrap.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>
#include <yt/yt/ytlib/api/native/helpers.h>

#include <yt/yt/ytlib/cell_master_client/cell_directory_synchronizer.h>

#include <yt/yt/ytlib/hive/cluster_directory_synchronizer.h>

#include <yt/yt/ytlib/misc/memory_usage_tracker.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/ytlib/queue_client/registration_manager.h>

#include <yt/yt/library/auth_server/authentication_manager.h>
#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/cypress_cookie_login.h>
#include <yt/yt/library/auth_server/cypress_cookie_manager.h>

#include <yt/yt/library/containers/disk_manager/config.h>
#include <yt/yt/library/containers/disk_manager/disk_info_provider.h>
#include <yt/yt/library/containers/disk_manager/disk_manager_proxy.h>

#include <yt/yt/library/monitoring/http_integration.h>
#include <yt/yt/library/monitoring/monitoring_manager.h>

#include <yt/yt/library/profiling/solomon/proxy.h>

#include <yt/yt/library/program/build_attributes.h>

#include <yt/yt/client/driver/driver.h>
#include <yt/yt/client/driver/config.h>

#include <yt/yt/client/logging/dynamic_table_log_writer.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>

#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytree/fluent.h>
#include <yt/yt/core/ytree/virtual.h>
#include <yt/yt/core/ytree/ypath_client.h>

#include <yt/yt/build/build.h>

namespace NYT::NHttpProxy {

using namespace NApi;
using namespace NAuth;
using namespace NConcurrency;
using namespace NDriver;
using namespace NHttp;
using namespace NMonitoring;
using namespace NNative;
using namespace NOrchid;
using namespace NProfiling;
using namespace NYson;
using namespace NYTree;
using namespace NAdmin;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TProxyConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger(), Config_);
    } else {
        WarnForUnrecognizedOptions(Logger(), Config_);
    }

    // TODO(gepardo): Pass native authenticator here.

    Control_ = New<TActionQueue>("Control");
    Poller_ = CreateThreadPoolPoller(Config_->ThreadCount, "Poller");
    Acceptor_ = CreateThreadPoolPoller(1, "Acceptor");

    MonitoringServer_ = NHttp::CreateServer(Config_->CreateMonitoringHttpServerConfig());

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(
        MonitoringServer_,
        Config_->SolomonExporter,
        &MonitoringManager_,
        &orchidRoot);

    SetBuildAttributes(
        orchidRoot,
        "http_proxy");

    DiskManagerProxy_ = CreateDiskManagerProxy(Config_->DiskManagerProxy);
    DiskInfoProvider_ = New<NContainers::TDiskInfoProvider>(
        DiskManagerProxy_,
        Config_->DiskInfoProvider);
    DiskChangeChecker_ = New<TDiskChangeChecker>(
        DiskInfoProvider_,
        GetControlInvoker(),
        Logger());

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;

    MemoryUsageTracker_ = CreateNodeMemoryTracker(
        *Config_->MemoryLimits->Total,
        /*limits*/ {},
        Logger(),
        HttpProxyProfiler.WithPrefix("/memory_usage"));

    ReconfigureMemoryLimits(Config_->MemoryLimits);

    Connection_ = CreateConnection(
        Config_->ClusterConnection,
        connectionOptions,
        /*clusterDirectoryOverride*/ {},
        MemoryUsageTracker_);

    SetupClusterConnectionDynamicConfigUpdate(
        Connection_,
        Config_->ClusterConnectionDynamicConfigPolicy,
        ConfigNode_->AsMap()->GetChildOrThrow("cluster_connection"),
        Logger());

    Connection_->GetClusterDirectorySynchronizer()->Start();
    // Force-start node directory synchronizer.
    Connection_->GetNodeDirectorySynchronizer()->Start();
    Connection_->GetQueueConsumerRegistrationManager()->StartSync();
    Connection_->GetMasterCellDirectorySynchronizer()->Start();
    SetupClients();

    Coordinator_ = New<TCoordinator>(Config_, this);

    auto setGlobalRoleTag = [] (const std::string& role) {
        TSolomonRegistry::Get()->SetDynamicTags({TTag{"proxy_role", role}});
    };
    setGlobalRoleTag(Coordinator_->GetSelf()->Role);
    Coordinator_->SubscribeOnSelfRoleChanged(BIND_NO_PROPAGATE(setGlobalRoleTag));

    DynamicConfigManager_ = CreateDynamicConfigManager(this);
    DynamicConfigManager_->SubscribeConfigChanged(BIND_NO_PROPAGATE(&TBootstrap::OnDynamicConfigChanged, MakeWeak(this)));

    if (Config_->ExposeConfigInOrchid) {
        SetNodeByYPath(
            orchidRoot,
            "/config",
            CreateVirtualNode(ConfigNode_));
        SetNodeByYPath(
            orchidRoot,
            "/dynamic_config_manager",
            CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));
        SetNodeByYPath(
            orchidRoot,
            "/cluster_connection",
            CreateVirtualNode(Connection_->GetOrchidService()));
    }
    SetNodeByYPath(
        orchidRoot,
        "/disk_monitoring",
        CreateVirtualNode(DiskChangeChecker_->GetOrchidService()));

    Config_->BusServer->Port = Config_->RpcPort;
    RpcServer_ = NRpc::NBus::CreateBusServer(CreateBusServer(Config_->BusServer));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker(),
        /*authenticator*/ nullptr));

    if (Config_->CoreDumper) {
        CoreDumper_ = NCoreDump::CreateCoreDumper(Config_->CoreDumper);
    }

    RpcServer_->RegisterService(CreateAdminService(
        GetControlInvoker(),
        CoreDumper_,
        /*authenticator*/ nullptr));

    HostsHandler_ = New<THostsHandler>(Coordinator_);
    ClusterConnectionHandler_ = New<TClusterConnectionHandler>(RootClient_);
    PingHandler_ = New<TPingHandler>(Coordinator_);
    DiscoverVersionsHandler_ = New<TDiscoverVersionsHandler>(
        RootClient_,
        TComponentDiscoveryOptions{.ProxyDeathAgeCallback = BIND(&TCoordinator::GetDeathAge, Coordinator_)});

    SolomonProxy_ = CreateSolomonProxy(
        Config_->SolomonProxy,
        TComponentDiscoveryOptions{.ProxyDeathAgeCallback = BIND(&TCoordinator::GetDeathAge, Coordinator_)},
        RootClient_,
        Poller_);

    ClickHouseHandler_ = New<NClickHouse::TClickHouseHandler>(this);
    ClickHouseHandler_->Start();

    if (Config_->ZookeeperProxy) {
        ZookeeperBootstrapProxy_ = CreateZookeeperBootstrapProxy(this);
        ZookeeperBootstrap_ = NZookeeperProxy::CreateBootstrap(ZookeeperBootstrapProxy_.get());
    }

    AccessChecker_ = CreateAccessChecker(this);

    auto driverV3Config = CloneYsonStruct(Config_->Driver);
    driverV3Config->ApiVersion = ApiVersion3;
    DriverV3_ = CreateDriver(Connection_, driverV3Config);

    auto driverV4Config = CloneYsonStruct(Config_->Driver);
    driverV4Config->ApiVersion = ApiVersion4;
    DriverV4_ = CreateDriver(Connection_, driverV4Config);

    AuthenticationManager_ = CreateAuthenticationManager(
        Config_->Auth,
        Poller_,
        RootClient_);

    if (Config_->Auth->CypressCookieManager) {
        CypressCookieLoginHandler_ = CreateCypressCookieLoginHandler(
            Config_->Auth->CypressCookieManager->CookieGenerator,
            RootClient_,
            AuthenticationManager_->GetCypressCookieManager()->GetCookieStore());
    }

    auto httpAuthenticator = New<THttpAuthenticator>(
        this,
        Config_->Auth,
        AuthenticationManager_);
    THashMap<int, THttpAuthenticatorPtr> authenticators{{Config_->HttpServer->Port, httpAuthenticator}};

    if (Config_->HttpsServer) {
        authenticators[Config_->HttpsServer->Port] = httpAuthenticator;
    }

    THttpAuthenticatorPtr tvmOnlyAuthenticator = nullptr;
    if (Config_->TvmOnlyAuth) {
        TvmOnlyAuthenticationManager_ = CreateAuthenticationManager(
            Config_->TvmOnlyAuth,
            Poller_,
            RootClient_);
        tvmOnlyAuthenticator = New<THttpAuthenticator>(
            this,
            Config_->TvmOnlyAuth,
            TvmOnlyAuthenticationManager_);
    }

    if (Config_->TvmOnlyHttpServer) {
        authenticators[Config_->TvmOnlyHttpServer->Port] = tvmOnlyAuthenticator;
    }

    if (Config_->TvmOnlyHttpsServer) {
        authenticators[Config_->TvmOnlyHttpsServer->Port] = tvmOnlyAuthenticator;
    }

    HttpAuthenticator_ = New<TCompositeHttpAuthenticator>(authenticators);

    Api_ = New<TApi>(this);
    Config_->HttpServer->ServerName = "HttpApi";
    ApiHttpServer_ = NHttp::CreateServer(
        Config_->HttpServer,
        Poller_,
        Acceptor_,
        WithCategory(MemoryUsageTracker_, EMemoryCategory::Other));
    RegisterRoutes(ApiHttpServer_);

    if (Config_->HttpsServer) {
        Config_->HttpsServer->ServerName = "HttpsApi";
        ApiHttpsServer_ = NHttps::CreateServer(
            Config_->HttpsServer,
            Poller_,
            Acceptor_,
            GetControlInvoker(),
            WithCategory(MemoryUsageTracker_, EMemoryCategory::Other));
        RegisterRoutes(ApiHttpsServer_);
    }

    if (Config_->TvmOnlyHttpServer) {
        Config_->TvmOnlyHttpServer->ServerName = "TvmOnlyHttpApi";
        TvmOnlyApiHttpServer_ = NHttp::CreateServer(
            Config_->TvmOnlyHttpServer,
            Poller_,
            Acceptor_,
            WithCategory(MemoryUsageTracker_, EMemoryCategory::Other));
        RegisterRoutes(TvmOnlyApiHttpServer_);
    }

    if (Config_->TvmOnlyHttpsServer) {
        Config_->TvmOnlyHttpsServer->ServerName = "TvmOnlyHttpsApi";
        TvmOnlyApiHttpsServer_ = NHttps::CreateServer(
            Config_->TvmOnlyHttpsServer,
            Poller_,
            Acceptor_,
            GetControlInvoker(),
            WithCategory(MemoryUsageTracker_, EMemoryCategory::Other));
        RegisterRoutes(TvmOnlyApiHttpsServer_);
    }

    if (Config_->ChytHttpServer) {
        Config_->ChytHttpServer->ServerName = "ChytHttpApi";
        ChytApiHttpServer_ = NHttp::CreateServer(Config_->ChytHttpServer, Poller_, Acceptor_);
        // Single handler.
        ChytApiHttpServer_->AddHandler("/", AllowCors(ClickHouseHandler_));
    }

    if (Config_->ChytHttpsServer) {
        Config_->ChytHttpsServer->ServerName = "ChytHttpsApi";
        ChytApiHttpsServer_ = NHttps::CreateServer(Config_->ChytHttpsServer, Poller_, Acceptor_, GetControlInvoker());
        ChytApiHttpsServer_->AddHandler("/", AllowCors(ClickHouseHandler_));
    }

    SetNodeByYPath(
        orchidRoot,
        "/http_proxy",
        CreateVirtualNode(Api_->CreateOrchidService()));

    HttpProxyHeapUsageProfiler_ = New<THttpProxyHeapUsageProfiler>(
        GetControlInvoker(),
        Config_->HeapProfiler);
}

bool TBootstrap::IsChytApiServerAddress(const NNet::TNetworkAddress& address) const
{
    return (ChytApiHttpServer_ && address == ChytApiHttpServer_->GetAddress())
        || (ChytApiHttpsServer_ && address == ChytApiHttpsServer_->GetAddress());
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::SetupClients()
{
    auto options = TClientOptions::FromUser(NSecurityClient::RootUserName);
    RootClient_ = Connection_->CreateNativeClient(options);

    NLogging::GetDynamicTableLogWriterFactory()->SetClient(RootClient_);
}

void TBootstrap::ReconfigureMemoryLimits(const TProxyMemoryLimitsConfigPtr& memoryLimits)
{
    MemoryUsageTracker_->SetTotalLimit(memoryLimits->Total.value_or(std::numeric_limits<i64>::max()));
}

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    ReconfigureNativeSingletons(Config_, newConfig);
    ReconfigureMemoryLimits(newConfig->MemoryLimits);

    DynamicConfig_.Store(newConfig);

    Connection_->Reconfigure(newConfig->ClusterConnection);

    Coordinator_->GetTraceSampler()->UpdateConfig(newConfig->Tracing);

    DiskManagerProxy_->OnDynamicConfigChanged(newConfig->DiskManagerProxy);
}

void TBootstrap::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    rsp->SetStatus(EStatusCode::OK);
    if (req->GetUrl().Path == "/service") {
        ReplyJson(rsp, [&] (NYson::IYsonConsumer* json) {
            BuildYsonFluently(json)
                .BeginMap()
                    .Item("start_time").Value(StartTime_)
                    .Item("version").Value(GetVersion())
                .EndMap();
        });
    } else {
        WaitFor(rsp->WriteBody(TSharedRef::FromString(GetVersion())))
            .ThrowOnError();
    }
}

void TBootstrap::Run()
{
    DynamicConfigManager_->Start();

    MonitoringServer_->Start();

    ApiHttpServer_->Start();
    if (ApiHttpsServer_) {
        ApiHttpsServer_->Start();
    }
    if (TvmOnlyApiHttpServer_) {
        TvmOnlyApiHttpServer_->Start();
    }
    if (TvmOnlyApiHttpsServer_) {
        TvmOnlyApiHttpsServer_->Start();
    }
    if (ChytApiHttpServer_) {
        ChytApiHttpServer_->Start();
    }
    if (ChytApiHttpsServer_) {
        ChytApiHttpsServer_->Start();
    }
    Coordinator_->Start();

    AuthenticationManager_->Start();

    RpcServer_->Start();

    if (ZookeeperBootstrap_) {
        ZookeeperBootstrap_->Run();
    }

    DiskChangeChecker_->Start();

    Sleep(TDuration::Max());
}

const IInvokerPtr& TBootstrap::GetControlInvoker() const
{
    return Control_->GetInvoker();
}

const TProxyConfigPtr& TBootstrap::GetConfig() const
{
    return Config_;
}

TProxyDynamicConfigPtr TBootstrap::GetDynamicConfig() const
{
    return DynamicConfig_.Acquire();
}

const NApi::NNative::IClientPtr& TBootstrap::GetRootClient() const
{
    return RootClient_;
}

const NApi::NNative::IConnectionPtr& TBootstrap::GetNativeConnection() const
{
    return Connection_;
}

const NDriver::IDriverPtr& TBootstrap::GetDriverV3() const
{
    return DriverV3_;
}

const NDriver::IDriverPtr& TBootstrap::GetDriverV4() const
{
    return DriverV4_;
}

const TCoordinatorPtr& TBootstrap::GetCoordinator() const
{
    return Coordinator_;
}

const IAccessCheckerPtr& TBootstrap::GetAccessChecker() const
{
    return AccessChecker_;
}

const TCompositeHttpAuthenticatorPtr& TBootstrap::GetHttpAuthenticator() const
{
    return HttpAuthenticator_;
}

const IAuthenticationManagerPtr& TBootstrap::GetAuthenticationManager() const
{
    return AuthenticationManager_;
}

const IDynamicConfigManagerPtr& TBootstrap::GetDynamicConfigManager() const
{
    return DynamicConfigManager_;
}

const IPollerPtr& TBootstrap::GetPoller() const
{
    return Poller_;
}

const TApiPtr& TBootstrap::GetApi() const
{
    return Api_;
}

IHttpHandlerPtr TBootstrap::AllowCors(IHttpHandlerPtr nextHandler) const
{
    return New<TCallbackHandler>(BIND_NO_PROPAGATE([config = Config_, nextHandler] (
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp)
    {
        if (MaybeHandleCors(req, rsp, config->Api->Cors)) {
            return;
        }

        nextHandler->HandleRequest(req, rsp);
    }));
}

void TBootstrap::RegisterRoutes(const NHttp::IServerPtr& server)
{
    server->AddHandler("/auth/whoami", AllowCors(HttpAuthenticator_));
    server->AddHandler("/api/", AllowCors(Api_));
    server->AddHandler("/hosts/", AllowCors(HostsHandler_));
    server->AddHandler("/cluster_connection/", AllowCors(ClusterConnectionHandler_));
    server->AddHandler("/ping/", AllowCors(PingHandler_));
    if (CypressCookieLoginHandler_) {
        server->AddHandler("/login/", CypressCookieLoginHandler_);
    }

    server->AddHandler("/internal/discover_versions/v2", AllowCors(DiscoverVersionsHandler_));

    SolomonProxy_->Register("/solomon_proxy", server);

    server->AddHandler("/version", AllowCors(MakeStrong(this)));
    server->AddHandler("/service", AllowCors(MakeStrong(this)));

    // ClickHouse.
    server->AddHandler("/query", AllowCors(ClickHouseHandler_));
    server->AddHandler("/chyt", AllowCors(ClickHouseHandler_));

    if (!Config_->UIRedirectUrl.empty()) {
        server->AddHandler("/", New<TCallbackHandler>(BIND([config = Config_] (
            const IRequestPtr& req,
            const IResponseWriterPtr& rsp)
        {
            if (req->GetUrl().Path == "/auth" || req->GetUrl().Path == "/auth/") {
                rsp->SetStatus(EStatusCode::SeeOther);
                rsp->GetHeaders()->Add("Location", "https://oauth.yt.yandex.net");
            } else if (req->GetUrl().Path == "/" || req->GetUrl().Path == "/ui") {
                rsp->SetStatus(EStatusCode::SeeOther);
                rsp->GetHeaders()->Add("Location", config->UIRedirectUrl + "?" + req->GetUrl().RawQuery);
            } else {
                rsp->SetStatus(EStatusCode::NotFound);
            }

            WaitFor(rsp->Close())
                .ThrowOnError();
        })));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
