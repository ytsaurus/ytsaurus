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
#include <yt/core/concurrency/action_queue.h>

#include <yt/core/ytree/fluent.h>

#include <yt/ytlib/api/native/connection.h>
#include <yt/ytlib/api/native/config.h>

#include <yt/ytlib/driver/driver.h>

#include <yt/ytlib/auth/authentication_manager.h>

namespace NYT {
namespace NHttpProxy {

using namespace NConcurrency;
using namespace NYTree;
using namespace NYson;
using namespace NHttp;
using namespace NApi;
using namespace NDriver;
using namespace NNative;

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

    BlackboxActionQueue_ = New<TActionQueue>("Blackbox");
    std::tie(TokenAuthenticator_, CookieAuthenticator_) = CreateAuthenticators(
        Config_->Auth,
        BlackboxActionQueue_->GetInvoker(),
        Client_);

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
            if (req->GetUrl().Path == "/" || req->GetUrl().Path == "/ui") {
                rsp->SetStatus(EStatusCode::SeeOther);
                rsp->GetHeaders()->Add("Location", config->UIRedirectUrl);
            } else {
                rsp->SetStatus(EStatusCode::NotFound);
            }

            WaitFor(rsp->Close())
                .ThrowOnError();
        })));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttpProxy
} // namespace NYT
