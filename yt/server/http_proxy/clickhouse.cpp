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

using namespace NApi;
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

            CliqueIdOrAlias_ = CgiParameters_.Get("database");

            if (CliqueIdOrAlias_.empty()) {
                ReplyWithError(
                    EStatusCode::NotFound,
                    TError("Clique id or alias should be specified using the `database` CGI parameter"));
            }

            YT_LOG_DEBUG("Clique id parsed (CliqueId: %v)", CliqueIdOrAlias_);

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

    bool TryPickRandomInstance(bool forceUpdate)
    {
        try {
            if (!TryFindDiscovery()) {
                return false;
            }

            Discovery_->UpdateList(Config_->CliqueCache->SoftAgeThreshold);
            WaitFor(Discovery_->UpdateList(forceUpdate ? Config_->ForceDiscoveryUpdateAgeThreshold : Config_->CliqueCache->HardAgeThreshold))
                .ThrowOnError();

            auto instances = Discovery_->List();
            if (instances.empty()) {
                RequestErrors_.emplace_back("Clique %v has no running instances", CliqueIdOrAlias_);
                return false;
            }
            auto it = instances.begin();
            std::advance(it, RandomNumber(instances.size()));
            const auto& [id, attributes] = *it;
            InstanceId_ = id;
            InstanceHost_ = attributes.at("host")->GetValue<TString>();
            auto port = attributes.at("http_port");
            InstanceHttpPort_ = (port->GetType() == ENodeType::String ? port->GetValue<TString>() : ToString(port->GetValue<ui64>()));

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
            RequestErrors_.push_back(TError("Failed to pick an instance")
                << ex);
            return false;
        }
    }

    bool TryIssueProxiedRequest(int retryIndex)
    {
        YT_LOG_DEBUG("Querying instance (Url: %v, RetryIndex: %v)", ProxiedRequestUrl_, retryIndex);
        auto responseOrError = WaitFor(HttpClient_->Post(ProxiedRequestUrl_, ProxiedRequestBody_, ProxiedRequestHeaders_));

        if (responseOrError.IsOK() && responseOrError.Value()->GetStatusCode() == EStatusCode::MovedPermanently) {
            responseOrError = TError("Instance moved, request rejected");
        }

        if (responseOrError.IsOK()) {
            ProxiedResponse_ = responseOrError.Value();
            YT_LOG_DEBUG("Got response from instance (StatusCode: %v, RetryIndex: %v)",
                ProxiedResponse_->GetStatusCode(),
                retryIndex);
            return true;
        } else {
            RequestErrors_.push_back(responseOrError);
            YT_LOG_DEBUG(responseOrError, "Proxied request failed (RetryIndex: %v)", retryIndex);
            Discovery_->Ban(InstanceId_);
            return false;
        }
    }

    void ReplyWithAllOccuredErrors(TError error) {
        ReplyWithError(EStatusCode::InternalServerError, error
            << RequestErrors_);
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

    void RemoveCliqueFromCache()
    {
        CliqueCache_->TryRemove(CliqueIdOrAlias_);
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
    // CliqueId or alias.
    TString CliqueIdOrAlias_;
    // Do TryResolveAliase() to set up this value.
    // Will be set automatically after finding Discovery in cache.
    std::optional<TString> CliqueId_;
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

    void SetCliqueId(TString cliqueId)
    {
        CliqueId_ = std::move(cliqueId);
        ProxiedRequestHeaders_->Add("X-Clique-Id", CliqueId_.value());
    }

    void ReplyWithError(EStatusCode statusCode, const TError& error) const
    {
        YT_LOG_DEBUG(error, "Request failed (StatusCode: %v)", statusCode);
        ReplyError(Response_, error);
    }

    bool TryResolveAlias()
    {
        if (CliqueId_) {
            return true;
        }

        if (!CliqueIdOrAlias_.StartsWith("*")) {
            SetCliqueId(CliqueIdOrAlias_);
            return true;
        }

        YT_LOG_DEBUG("Resolving alias (Alias: %v)", CliqueIdOrAlias_);
        try {
            TGetNodeOptions options;
            options.Timeout = Config_->AliasResolutionTimeout;
            auto operationId = ConvertTo<TGuid>(WaitFor(
                Client_->GetNode(
                    Format("//sys/scheduler/orchid/scheduler/operations/%v/operation_id",
                        ToYPathLiteral(CliqueIdOrAlias_)),
                    options))
                    .ValueOrThrow());
            SetCliqueId(ToString(operationId));
        } catch (const std::exception& ex) {
            RequestErrors_.emplace_back(TError("Error while resolving alias %Qv", CliqueIdOrAlias_)
                << ex);
            return false;
        }

        YT_LOG_DEBUG("Alias resolved (Alias: %v, CliqueId: %v)", CliqueIdOrAlias_, CliqueId_);

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

    bool TryFindDiscovery()
    {
        if (Discovery_) {
            return true;
        }

        try {
            auto cookie = CliqueCache_->BeginInsert(CliqueIdOrAlias_);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Clique cache missed (CliqueId: %v)", CliqueIdOrAlias_);

                if (!TryResolveAlias()) {
                    return false;
                }

                TString path = Config_->DiscoveryPath + "/" + CliqueId_.value();
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
                    CliqueIdOrAlias_,
                    CliqueId_.value(),
                    config,
                    Client_,
                    ControlInvoker_,
                    std::vector<TString>{"host", "http_port"},
                    Logger));
            }

            Discovery_ = WaitFor(cookie.GetValue())
                .ValueOrThrow();

            if (!CliqueId_) {
                SetCliqueId(Discovery_->GetCliqueId());
            }

        } catch (const std::exception& ex) {
            RequestErrors_.push_back(TError("Failed to create discovery")
                << ex);
            return false;
        }

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
        // Force update have already been done.
        bool forceUpdated = false;
        // An instance was picked successfully on previous step or no step has happened yet.
        bool pickedInstance = true;
        for (int retry = 0; retry <= Config_->DeadInstanceRetryCount; ++retry) {
            bool forceUpdate = retry > Config_->RetryWithoutUpdateLimit;
            // If we did not find any instances on previous step, we need to do force update right now.
            if (!pickedInstance) {
                forceUpdate = true;
            }
            // We don't allow to do force update several times per one request.
            if (forceUpdated) {
                forceUpdate = false;
            }

            pickedInstance = false;
            if (!context->TryPickRandomInstance(forceUpdate)) {
                // There is no chance to invoke the request if we can not pick an instance even after force update.
                if (forceUpdated) {
                    // We may have banned all instances because of network problems or we have resolved CliqueId incorrectly
                    // (possibly due to clique restart under same alias), and cached discovery is not relevant any more.
                    context->RemoveCliqueFromCache();
                    break;
                }
                continue;
            }
            pickedInstance = true;

            if (context->TryIssueProxiedRequest(retry)) {
                success = true;
                break;
            }
        }

        if (!success) {
            context->ReplyWithAllOccuredErrors(TError("Request failed"));
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
