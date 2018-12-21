#include "bootstrap.h"

#include "config.h"
#include "coordinator.h"
#include "api.h"
#include "http_authenticator.h"
#include "private.h"

#include <yt/build/build.h>

#include <yt/core/http/server.h>
#include <yt/core/http/helpers.h>
#include <yt/core/https/server.h>

#include <yt/core/concurrency/thread_pool_poller.h>
#include <yt/core/concurrency/thread_pool.h>

#include <yt/core/ytree/fluent.h>
#include <yt/core/ytree/virtual.h>
#include <yt/core/ytree/ypath_client.h>

#include <yt/core/profiling/profile_manager.h>

#include <yt/core/misc/ref_counted_tracker.h>
#include <yt/core/misc/ref_counted_tracker_statistics_producer.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/program/build_attributes.h>

#include <yt/ytlib/monitoring/http_integration.h>
#include <yt/ytlib/monitoring/monitoring_manager.h>

#include <yt/ytlib/auth/authentication_manager.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NApi;
using namespace NDriver;
using namespace NNative;
using namespace NMonitoring;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = HttpProxyLogger;

////////////////////////////////////////////////////////////////////////////////

TBootstrap::TBootstrap(TProxyConfigPtr config, INodePtr configNode)
    : Config_(std::move(config))
    , ConfigNode_(std::move(configNode))
{
    WarnForUnrecognizedOptions(Logger, Config_);

    Control_ = New<TActionQueue>("Control");
    Poller_ = CreateThreadPoolPoller(Config_->ThreadCount, "Poller");

    Config_->MonitoringServer->Port = Config_->MonitoringPort;
    MonitoringServer_ = NHttp::CreateServer(
        Config_->MonitoringServer);

    MonitoringManager_ = New<TMonitoringManager>();
    MonitoringManager_->Register(
        "/ref_counted",
        CreateRefCountedTrackerStatisticsProducer());
    MonitoringManager_->Start();
    
    auto orchidRoot = NYTree::GetEphemeralNodeFactory(true)->CreateMap();
    SetNodeByYPath(
        orchidRoot,
        "/monitoring",
        CreateVirtualNode(MonitoringManager_->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/profiling",
        CreateVirtualNode(TProfileManager::Get()->GetService()));
    SetNodeByYPath(
        orchidRoot,
        "/config",
        ConfigNode_);

    MonitoringServer_->AddHandler(
        "/orchid/",
        NMonitoring::GetOrchidYPathHttpHandler(orchidRoot));

    SetBuildAttributes(orchidRoot, "http_proxy");

    Connection_ = CreateConnection(ConvertTo<NNative::TConnectionConfigPtr>(Config_->Driver));
    TClientOptions options;
    options.PinnedUser = NSecurityClient::RootUserName;
    Client_ = Connection_->CreateClient(options);

    Coordinator_ = New<TCoordinator>(Config_, this);
    HostsHandler_ = New<THostsHandler>(Coordinator_);
    PingHandler_ = New<TPingHandler>(Coordinator_);
    DiscoverVersionsHandler_ = New<TDiscoverVersionsHandler>(Connection_, Client_);

    auto driverV3Config = CloneNode(Config_->Driver);
    driverV3Config->AsMap()->AddChild("api_version", ConvertToNode<i64>(3));
    DriverV3_ = CreateDriver(Connection_, ConvertTo<TDriverConfigPtr>(driverV3Config));

    auto driverV4Config = CloneNode(Config_->Driver);
    driverV4Config->AsMap()->AddChild("api_version", ConvertToNode<i64>(4));
    DriverV4_ = CreateDriver(Connection_, ConvertTo<TDriverConfigPtr>(driverV4Config));

    auto authenticationManager = New<NAuth::TAuthenticationManager>(
        Config_->Auth,
        Poller_,
        Client_);
    TokenAuthenticator_ = authenticationManager->GetTokenAuthenticator();
    CookieAuthenticator_ = authenticationManager->GetCookieAuthenticator();

    HttpAuthenticator_ = New<THttpAuthenticator>(Config_->Auth, TokenAuthenticator_, CookieAuthenticator_);

    Api_ = New<TApi>(this);
    ApiHttpServer_ = NHttp::CreateServer(Config_->HttpServer, Poller_);
    RegisterRoutes(ApiHttpServer_);

    if (Config_->HttpsServer) {
        ApiHttpsServer_ = NHttps::CreateServer(Config_->HttpsServer, Poller_);
        RegisterRoutes(ApiHttpsServer_);
    }
}

TBootstrap::~TBootstrap() = default;

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
    return Client_;
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

void TBootstrap::RegisterRoutes(const NHttp::IServerPtr& server)
{
    server->AddHandler("/auth/whoami", HttpAuthenticator_);
    server->AddHandler("/api/", Api_);
    server->AddHandler("/hosts/", HostsHandler_);
    server->AddHandler("/ping/", PingHandler_);

    server->AddHandler("/internal/discover_versions", DiscoverVersionsHandler_);
    // Legacy.
    server->AddHandler("/api/v3/_discover_versions", DiscoverVersionsHandler_);

    server->AddHandler("/version", MakeStrong(this));
    server->AddHandler("/service", MakeStrong(this));

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
