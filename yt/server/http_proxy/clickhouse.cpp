#include "clickhouse.h"

#include "bootstrap.h"
#include "clique_cache.h"
#include "config.h"
#include "coordinator.h"

#include <yt/ytlib/auth/token_authenticator.h>

#include <yt/client/api/client.h>

#include <yt/core/http/client.h>

#include <yt/core/logging/log.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profile_manager.h>

#include <library/string_utils/base64/base64.h>

#include <library/cgiparam/cgiparam.h>
#include <util/string/vector.h>

#include <util/random/random.h>

namespace NYT::NHttpProxy {

using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NProfiling;
using namespace NLogging;
using namespace NYPath;

////////////////////////////////////////////////////////////////////////////////

TLogger ClickHouseLogger("ClickHouseProxy");
TProfiler ClickHouseProfiler("/clickhouse_proxy");

////////////////////////////////////////////////////////////////////////////////

class TClickHouseContext
    : public TIntrinsicRefCounted
{
public:
    TClickHouseContext(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TClickHouseConfigPtr& config,
        const NAuth::ITokenAuthenticatorPtr& tokenAuthenticator,
        const NApi::IClientPtr& client,
        const NHttp::IClientPtr& httpClient,
        const TCliqueCachePtr cliqueCache,
        IInvokerPtr controlInvoker)
        : Logger(TLogger(ClickHouseLogger).AddTag("RequestId: %v", req->GetRequestId()))
        , Request_(req)
        , Response_(rsp)
        , Config_(config)
        , TokenAuthenticator_(tokenAuthenticator)
        , Client_(client)
        , HttpClient_(httpClient)
        , CliqueCache_(cliqueCache)
        , ControlInvoker_(controlInvoker)
    { }

    bool TryPrepare()
    {
        try {
            CgiParameters_ = TCgiParameters(Request_->GetUrl().RawQuery);

            if (!TryProcessHeaders()) {
                return false;
            }

            if (!TryAuthenticate()) {
                return false;
            }

            CliqueId_ = CgiParameters_.Get("database");

            if (CliqueId_.empty()) {
                ReplyWithError(
                    EStatusCode::NotFound,
                    TError("Clique id or alias should be specified using the `database` CGI parameter"));
            }

            if (CliqueId_.StartsWith("*")) {
                if (!TryResolveAlias()) {
                    return false;
                }
            }

            YT_LOG_DEBUG("Clique id parsed (CliqueId: %v)", CliqueId_);

            // TODO(max42): remove this when DataLens makes proper authorization. Duh.
            if (auto* header = Request_->GetHeaders()->Find("X-DataLens-Real-User")) {
                YT_LOG_DEBUG("Header contains DataLens real username (RealUser: %v)", *header);
            }

            ProxiedRequestBody_ = Request_->ReadAll();
            if (!ProxiedRequestBody_) {
                ReplyWithError(EStatusCode::BadRequest, TError("Body should not be empty"));
                return false;
            }

            ProxiedRequestHeaders_ = Request_->GetHeaders()->Duplicate();
            ProxiedRequestHeaders_->Remove("Authorization");
            ProxiedRequestHeaders_->Add("X-Yt-User", User_);
            ProxiedRequestHeaders_->Add("X-Clickhouse-User", User_);
            ProxiedRequestHeaders_->Add("X-Yt-Request-Id", ToString(Request_->GetRequestId()));

            CgiParameters_.EraseAll("database");
            CgiParameters_.EraseAll("query_id");
            CgiParameters_.emplace("query_id", ToString(Request_->GetRequestId()));
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Preparation failed")
                << ex);
            return false;
        }
        return true;
    }

    bool TryPickRandomInstance()
    {
        try {
            auto cookie = CliqueCache_->BeginInsert(CliqueId_);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Clique cache missed (CliqueId: %v)", CliqueId_);

                TString path = Config_->DiscoveryPath + "/" + CliqueId_;
                NApi::TGetNodeOptions options;
                options.ReadFrom = NApi::EMasterChannelKind::Cache;
                auto node = ConvertToNode(WaitFor(Client_->GetNode(path + "/@", options))
                    .ValueOrThrow())->AsMap()->FindChild("discovery_version");
                i64 version = (node ? node->GetValue<i64>() : 0);

                auto config = New<TDiscoveryConfig>();
                config->Directory = path;
                config->BanTimeout = Config_->CliqueCache->UnavailableInstanceBanTimeout;
                config->ReadFrom = NApi::EMasterChannelKind::Cache;
                config->MasterCacheExpireTime = Config_->CliqueCache->MasterCacheExpireTime;
                if (version == 0) {
                    config->SkipUnlockedParticipants = false;
                }

                cookie.EndInsert(New<TCachedDiscovery>(
                    CliqueId_,
                    config,
                    Client_,
                    ControlInvoker_,
                    std::vector<TString>{"host", "http_port"},
                    Logger));
            }

            auto discovery = WaitFor(cookie.GetValue())
                .ValueOrThrow();

            discovery->UpdateList(Config_->CliqueCache->SoftAgeThreshold);
            WaitFor(discovery->UpdateList(Config_->CliqueCache->HardAgeThreshold))
                .ThrowOnError();

            auto instances = discovery->List();
            if (instances.empty()) {
                ReplyWithError(EStatusCode::NotFound, TError("Clique %v has no running instances", CliqueId_)
                    << RequestErrors_);
                return false;
            }
            auto it = instances.begin();
            std::advance(it, RandomNumber(instances.size()));
            const auto& [id, attributes] = *it;
            InstanceId_ = id;
            InstanceHost_ = attributes.at("host")->GetValue<TString>();
            auto port = attributes.at("http_port");
            InstanceHttpPort_ = (port->GetType() == ENodeType::String ? port->GetValue<TString>() : ToString(port->GetValue<ui64>()));
            Discovery_ = std::move(discovery);

            YT_LOG_DEBUG("Forwarding query to a randomly chosen instance (InstanceId: %v, Host: %v, HttpPort: %v)",
                InstanceId_,
                InstanceHost_,
                InstanceHttpPort_);

            ProxiedRequestUrl_ = Format("http://%v:%v%v?%v",
                InstanceHost_,
                InstanceHttpPort_,
                Request_->GetUrl().Path,
                CgiParameters_.Print());

            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Failed to pick an instance")
                << ex << RequestErrors_);
            return false;
        }
    }

    bool TryIssueProxiedRequest(int retryIndex)
    {
        YT_LOG_DEBUG("Querying instance (Url: %v, RetryIndex: %v)", ProxiedRequestUrl_, retryIndex);
        auto responseOrError = WaitFor(HttpClient_->Post(ProxiedRequestUrl_, ProxiedRequestBody_, ProxiedRequestHeaders_));
        if (responseOrError.IsOK()) {
            ProxiedResponse_ = responseOrError.Value();
            YT_LOG_DEBUG("Got response from instance (StatusCode: %v)", ProxiedResponse_->GetStatusCode());
            return true;
        } else {
            RequestErrors_.push_back(responseOrError);
            if (retryIndex == Config_->DeadInstanceRetryCount) {
                ReplyWithError(EStatusCode::InternalServerError, TError("Proxied request failed")
                    << RequestErrors_);
            } else {
                YT_LOG_DEBUG(responseOrError, "Proxied request failed, retrying (RetryIndex: %v)", retryIndex);
            }
            Discovery_->Ban(InstanceId_);
            return false;
        }
    }

    void ForwardProxiedResponse()
    {
        YT_LOG_DEBUG("Getting proxied status code");
        auto statusCode = ProxiedResponse_->GetStatusCode();
        Response_->SetStatus(statusCode);
        YT_LOG_DEBUG("Received status code, getting proxied headers (StatusCode: %v)", statusCode);
        Response_->GetHeaders()->MergeFrom(ProxiedResponse_->GetHeaders());
        YT_LOG_DEBUG("Received headers, forwarding proxied response");
        PipeInputToOutput(ProxiedResponse_, Response_);
        WaitFor(Response_->Close())
            .ThrowOnError();
        YT_LOG_DEBUG("Proxied response forwarded");
    }

    const TString& GetUser() const
    {
        return User_;
    }

private:
    TLogger Logger;
    const IRequestPtr& Request_;
    const IResponseWriterPtr& Response_;
    const TClickHouseConfigPtr& Config_;
    const NAuth::ITokenAuthenticatorPtr& TokenAuthenticator_;
    const NApi::IClientPtr& Client_;
    const NHttp::IClientPtr& HttpClient_;
    const TCliqueCachePtr CliqueCache_;
    IInvokerPtr ControlInvoker_;

    // These fields contain the request details after parsing CGI params and headers.
    TCgiParameters CgiParameters_;
    TString CliqueId_;
    TString Token_;
    TString User_;
    TString InstanceId_;
    TString InstanceHost_;
    TString InstanceHttpPort_;
    TCachedDiscoveryPtr Discovery_;

    // These fields define the proxied request issued to a randomly chosen instance.
    TString ProxiedRequestUrl_;
    TSharedRef ProxiedRequestBody_;
    THeadersPtr ProxiedRequestHeaders_;

    //! Response from a chosen instance.
    IResponsePtr ProxiedResponse_;

    std::vector<TError> RequestErrors_;

    void ReplyWithError(EStatusCode statusCode, const TError& error) const
    {
        YT_LOG_DEBUG(error, "Request failed (StatusCode: %v)", statusCode);
        ReplyError(Response_, error);
    }

    bool TryResolveAlias()
    {
        auto alias = CliqueId_;
        YT_LOG_DEBUG("Resolving alias (Alias: %v)", alias);
        try {
            auto operationId = ConvertTo<TGuid>(WaitFor(
                Client_->GetNode(
                    Format("//sys/scheduler/orchid/scheduler/operations/%v/operation_id",
                    ToYPathLiteral(alias))))
                    .ValueOrThrow());
            CliqueId_ = ToString(operationId);
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::NotFound, TError("Error while resolving alias %Qv", alias)
                << ex);
            return false;
        }

        YT_LOG_DEBUG("Alias resolved (Alias: %v, CliqueId: %v)", alias, CliqueId_);
        CgiParameters_.ReplaceUnescaped("database", CliqueId_);

        return true;
    }

    void ParseTokenFromAuthorizationHeader(const TString& authorization) {
        YT_LOG_DEBUG("Parsing token from Authorization header");
        // Two supported Authorization kinds are "Basic <base64(clique-id:oauth-token)>" and "OAuth <oauth-token>".
        auto authorizationTypeAndCredentials = SplitString(authorization, " ", 2);
        const auto& authorizationType = authorizationTypeAndCredentials[0];
        if (authorizationType == "OAuth" && authorizationTypeAndCredentials.size() == 2) {
            Token_ = authorizationTypeAndCredentials[1];
        } else if (authorizationType == "Basic" && authorizationTypeAndCredentials.size() == 2) {
            const auto& credentials = authorizationTypeAndCredentials[1];
            auto fooAndToken = SplitString(Base64Decode(credentials), ":", 2);
            if (fooAndToken.size() == 2) {
                // First component (that should be username) is ignored.
                Token_ = fooAndToken[1];
            } else {
                ReplyWithError(
                    EStatusCode::Unauthorized,
                    TError("Wrong 'Basic' authorization header format; 'default:<oauth-token>' encoded with base64 expected"));
                return;
            }
        } else {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Unsupported type of authorization header (AuthorizationType: %v, TokenCount: %v)",
                       authorizationType,
                       authorizationTypeAndCredentials.size()));
            return;
        }
        YT_LOG_DEBUG("Token parsed (AuthorizationType: %v)", authorizationType);

    }

    bool TryProcessHeaders()
    {
        const auto* authorization = Request_->GetHeaders()->Find("Authorization");
        if (authorization && !authorization->empty()) {
            ParseTokenFromAuthorizationHeader(*authorization);
        } else if (CgiParameters_.Has("password")) {
            Token_ = CgiParameters_.Get("password");
            CgiParameters_.EraseAll("password");
            CgiParameters_.EraseAll("user");
        } else if (!Config_->IgnoreMissingCredentials) {
            ReplyWithError(EStatusCode::Unauthorized,
                TError("Authorization should be perfomed either by setting `Authorization` header (`Basic` or `OAuth` schemes) "
                       "or `password` CGI parameter"));
            return false;
        }

        return true;
    }

    bool TryAuthenticate()
    {
        try {
            NAuth::TTokenCredentials credentials;
            credentials.Token = Token_;
            User_ = WaitFor(TokenAuthenticator_->Authenticate(credentials))
                .ValueOrThrow()
                .Login;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Authorization failed")
                    << ex);
            return false;
        }

        YT_LOG_DEBUG("User authenticated (User: %v)", User_);
        return true;
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseContext);
DECLARE_REFCOUNTED_CLASS(TClickHouseContext);

////////////////////////////////////////////////////////////////////////////////

TClickHouseHandler::TClickHouseHandler(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Coordinator_(bootstrap->GetCoordinator())
    , Config_(Bootstrap_->GetConfig()->ClickHouse)
    , HttpClient_(CreateClient(Config_->HttpClient, Bootstrap_->GetPoller()))
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
    , CliqueCache_(New<TCliqueCache>(Config_->CliqueCache))
{
    if (!Bootstrap_->GetConfig()->Auth->RequireAuthentication) {
        Config_->IgnoreMissingCredentials = true;
    }
    ProfilingExecutor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TClickHouseHandler::OnProfiling, MakeWeak(this)),
        Config_->ProfilingPeriod);
    ProfilingExecutor_->Start();
}

void TClickHouseHandler::HandleRequest(
    const IRequestPtr& request,
    const IResponseWriterPtr& response)
{
    if (!Coordinator_->CanHandleHeavyRequests()) {
        // We intentionally read the body of the request and drop it to make sure
        // that client does not block on writing the body.
        request->ReadAll();
        RedirectToDataProxy(request, response, Coordinator_);
    } else {
        ProcessDebugHeaders(request, response, Coordinator_);
        auto context = New<TClickHouseContext>(
            request,
            response,
            Config_,
            Bootstrap_->GetTokenAuthenticator(),
            Bootstrap_->GetClickHouseClient(),
            HttpClient_,
            CliqueCache_,
            ControlInvoker_);
        
        if (!context->TryPrepare()) {
            // TODO(max42): profile something here.
            return;
        }

        bool success = false; 
        for (int retry = 0; retry <= Config_->DeadInstanceRetryCount; ++retry) {
            if (!context->TryPickRandomInstance()) {
                return;
            }
            if (context->TryIssueProxiedRequest(retry)) {
                success = true;
                break;
            }
        }

        if (!success) {
            return;
        }

        ControlInvoker_->Invoke(BIND(&TClickHouseHandler::AdjustQueryCount, MakeWeak(this), context->GetUser(), +1));
        context->ForwardProxiedResponse();
        ControlInvoker_->Invoke(BIND(&TClickHouseHandler::AdjustQueryCount, MakeWeak(this), context->GetUser(), -1));
    }
}

void TClickHouseHandler::AdjustQueryCount(const TString& user, int delta)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    THashMap<TString, int>::insert_ctx ctx;
    auto it = UserToRunningQueryCount_.find(user, ctx);
    if (it == UserToRunningQueryCount_.end()) {
        it = UserToRunningQueryCount_.emplace_direct(ctx, user, delta);
    } else {
        it->second += delta;
    }
    YT_VERIFY(it->second >= 0);
    if (it->second == 0) {
        UserToRunningQueryCount_.erase(it);
    }
}

void TClickHouseHandler::OnProfiling()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    const auto& Logger = ClickHouseLogger;

    YT_LOG_DEBUG("Flushing profiling");

    for (auto& [user, runningQueryCount] : UserToRunningQueryCount_) {
        ClickHouseProfiler.Enqueue(
            "/running_query_count",
            runningQueryCount,
            EMetricType::Gauge,
            {TProfileManager::Get()->RegisterTag("user", user)});
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy
