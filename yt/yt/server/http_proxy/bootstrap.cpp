#include "bootstrap.h"

#include "config.h"
#include "coordinator.h"
#include "api.h"
#include "http_authenticator.h"
#include "private.h"

#include <yt/server/http_proxy/clickhouse/handler.h>

#include <yt/ytlib/api/native/config.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/auth/authentication_manager.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/node_tracker_client/node_directory_synchronizer.h>

#include <yt/ytlib/orchid/orchid_service.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/client/driver/driver.h>
#include <yt/client/driver/config.h>

#include <yt/core/bus/tcp/server.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/http/helpers.h>
#include <yt/core/http/server.h>

#include <yt/core/https/config.h>
#include <yt/core/https/server.h>

#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>
#include <yt/core/misc/ref_counted_tracker.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/rpc/bus/server.h>

#include <yt/core/ytalloc/statistics_producer.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/build/build.h>

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

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    Config_->MonitoringServer->ServerName = "monitoring";
    MonitoringServer_ = NHttp::CreateServer(
        Config_->MonitoringServer);

    NYTree::IMapNodePtr orchidRoot;
    NMonitoring::Initialize(MonitoringServer_, &MonitoringManager_, &orchidRoot);

    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);

    SetBuildAttributes(orchidRoot, "http_proxy");

    NNative::TConnectionOptions connectionOptions;
    connectionOptions.RetryRequestQueueSizeLimitExceeded = Config_->RetryRequestQueueSizeLimitExceeded;
    Connection_ = CreateConnection(
        ConvertTo<NNative::TConnectionConfigPtr>(Config_->Driver),
        connectionOptions);
    // Force-start node directory synchronizer.
    Connection_->GetNodeDirectorySynchronizer()->Start();
    SetupClients();

    Coordinator_ = New<TCoordinator>(Config_, this);

    SetNodeByYPath(
        orchidRoot,
        "/coordinator",
        CreateVirtualNode(Coordinator_->CreateOrchidService()));

    auto setGlobalRoleTag = [] (const TString& role) {
        auto id = TProfileManager::Get()->RegisterTag("proxy_role", role);
        TProfileManager::Get()->SetGlobalTag(id);
    };
    setGlobalRoleTag(Coordinator_->GetSelf()->Role);
    Coordinator_->SubscribeOnSelfRoleChanged(BIND(setGlobalRoleTag));

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

    HttpAuthenticator_ = New<THttpAuthenticator>(
        Config_->Auth,
        TokenAuthenticator_,
        CookieAuthenticator_,
        Coordinator_);

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
    {
        TClientOptions options;
        options.PinnedUser = NSecurityClient::RootUserName;
        RootClient_ = Connection_->CreateClient(options);
    }
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

const NApi::IClientPtr& TBootstrap::GetRootClient() const
{
    return RootClient_;
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

const THttpAuthenticatorPtr& TBootstrap::GetHttpAuthenticator() const
{
    return HttpAuthenticator_;
}

const ITokenAuthenticatorPtr& TBootstrap::GetTokenAuthenticator() const
{
    return TokenAuthenticator_;
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
