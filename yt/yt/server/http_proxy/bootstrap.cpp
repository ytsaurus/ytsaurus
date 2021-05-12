#include "bootstrap.h"

#include "access_checker.h"
#include "config.h"
#include "coordinator.h"
#include "dynamic_config_manager.h"
#include "api.h"
#include "http_authenticator.h"
#include "private.h"

#include <yt/yt/server/http_proxy/clickhouse/handler.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/auth/authentication_manager.h>

#include <yt/yt/ytlib/monitoring/http_integration.h>
#include <yt/yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/yt/ytlib/orchid/orchid_service.h>

#include <yt/yt/ytlib/program/build_attributes.h>
#include <yt/yt/ytlib/program/helpers.h>

#include <yt/yt/client/driver/driver.h>
#include <yt/yt/client/driver/config.h>

#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/concurrency/thread_pool_poller.h>
#include <yt/yt/core/concurrency/thread_pool.h>

#include <yt/yt/core/http/helpers.h>
#include <yt/yt/core/http/server.h>

#include <yt/yt/core/https/config.h>
#include <yt/yt/core/https/server.h>

#include <yt/yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/yt/core/misc/ref_counted_tracker.h>

#include <yt/yt/core/profiling/profile_manager.h>

#include <yt/yt/core/rpc/bus/server.h>

#include <yt/yt/core/ytalloc/statistics_producer.h>

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

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TProxyConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    if (Config_->AbortOnUnrecognizedOptions) {
        AbortOnUnrecognizedOptions(Logger, Config_);
    } else {
        WarnForUnrecognizedOptions(Logger, Config_);
    }

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

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);
    SetBuildAttributes(
        orchidRoot,
        "http_proxy");

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    Connection_ = CreateConnection(
        ConvertTo<NNative::TConnectionConfigPtr>(Config_->Driver),
        connectionOptions);
    // Force-start node directory synchronizer.
    Connection_->GetNodeDirectorySynchronizer()->Start();
    SetupClients();

    Coordinator_ = New<TCoordinator>(Config_, this);

    auto setGlobalRoleTag = [] (const TString& role) {
        TSolomonRegistry::Get()->SetDynamicTags({TTag{"proxy_role", role}});
    };
    setGlobalRoleTag(Coordinator_->GetSelf()->Role);
    Coordinator_->SubscribeOnSelfRoleChanged(BIND(setGlobalRoleTag));

    DynamicConfigManager_ = CreateDynamicConfigManager(this);
    DynamicConfigManager_->SubscribeConfigChanged(BIND(&TBootstrap::OnDynamicConfigChanged, MakeWeak(this)));

    SetNodeByYPath(
        orchidRoot,
        "/dynamic_config_manager",
        CreateVirtualNode(DynamicConfigManager_->GetOrchidService()));

    Config_->BusServer->Port = Config_->RpcPort;
    RpcServer_ = NRpc::NBus::CreateBusServer(CreateTcpBusServer(Config_->BusServer));

    RpcServer_->RegisterService(CreateOrchidService(
        orchidRoot,
        GetControlInvoker()));

    HostsHandler_ = New<THostsHandler>(Coordinator_);
    PingHandler_ = New<TPingHandler>(Coordinator_);
    DiscoverVersionsHandlerV1_ = New<TDiscoverVersionsHandlerV1>(Connection_, RootClient_, Config_->Coordinator);
    DiscoverVersionsHandlerV2_ = New<TDiscoverVersionsHandlerV2>(Connection_, RootClient_, Config_->Coordinator);

    ClickHouseHandler_ = New<NClickHouse::TClickHouseHandler>(this);

    AccessChecker_ = CreateAccessChecker(this);

    auto driverV3Config = CloneNode(Config_->Driver);
    driverV3Config->AsMap()->AddChild("api_version", ConvertToNode<i64>(3));
    DriverV3_ = CreateDriver(Connection_, ConvertTo<TDriverConfigPtr>(driverV3Config));

    auto driverV4Config = CloneNode(Config_->Driver);
    driverV4Config->AsMap()->AddChild("api_version", ConvertToNode<i64>(4));
    DriverV4_ = CreateDriver(Connection_, ConvertTo<TDriverConfigPtr>(driverV4Config));

    auto authenticationManager = New<TAuthenticationManager>(
        Config_->Auth,
        Poller_,
        RootClient_);
    TokenAuthenticator_ = authenticationManager->GetTokenAuthenticator();
    CookieAuthenticator_ = authenticationManager->GetCookieAuthenticator();

    HttpAuthenticator_ = New<THttpAuthenticator>(this);

    Api_ = New<TApi>(this);
    Config_->HttpServer->ServerName = "http_api";
    ApiHttpServer_ = NHttp::CreateServer(Config_->HttpServer, Poller_, Acceptor_);
    RegisterRoutes(ApiHttpServer_);

    if (Config_->HttpsServer) {
        Config_->HttpsServer->ServerName = "https_api";
        ApiHttpsServer_ = NHttps::CreateServer(Config_->HttpsServer, Poller_, Acceptor_);
        RegisterRoutes(ApiHttpsServer_);
    }
}

TBootstrap::~TBootstrap() = default;

void TBootstrap::SetupClients()
{
    auto options = TClientOptions::FromUser(NSecurityClient::RootUserName);
    RootClient_ = Connection_->CreateClient(options);
}

void TBootstrap::OnDynamicConfigChanged(
    const TProxyDynamicConfigPtr& /*oldConfig*/,
    const TProxyDynamicConfigPtr& newConfig)
{
    ReconfigureSingletons(Config_, newConfig);

    DynamicConfig_.Store(newConfig);
}

void TBootstrap::HandleRequest(
    const NHttp::IRequestPtr& req,
    const NHttp::IResponseWriterPtr& rsp)
{
    if (MaybeHandleCors(req, rsp)) {
        return;
    }

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
    Coordinator_->Start();

    RpcServer_->Start();

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
    return DynamicConfig_.Load();
}

const NApi::IClientPtr& TBootstrap::GetRootClient() const
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

const THttpAuthenticatorPtr& TBootstrap::GetHttpAuthenticator() const
{
    return HttpAuthenticator_;
}

const ITokenAuthenticatorPtr& TBootstrap::GetTokenAuthenticator() const
{
    return TokenAuthenticator_;
}

const ICookieAuthenticatorPtr& TBootstrap::GetCookieAuthenticator() const
{
    return CookieAuthenticator_;
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

void TBootstrap::RegisterRoutes(const NHttp::IServerPtr& server)
{
    server->AddHandler("/auth/whoami", HttpAuthenticator_);
    server->AddHandler("/api/", Api_);
    server->AddHandler("/hosts/", HostsHandler_);
    server->AddHandler("/ping/", PingHandler_);

    server->AddHandler("/internal/discover_versions", DiscoverVersionsHandlerV1_);
    server->AddHandler("/internal/discover_versions/v2", DiscoverVersionsHandlerV2_);
    // Legacy.
    server->AddHandler("/api/v3/_discover_versions", DiscoverVersionsHandlerV1_);

    server->AddHandler("/version", MakeStrong(this));
    server->AddHandler("/service", MakeStrong(this));

    // ClickHouse.
    server->AddHandler("/query", ClickHouseHandler_);

    if (!Config_->UIRedirectUrl.empty()) {
        server->AddHandler("/", New<TCallbackHandler>(BIND([config=Config_] (
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
