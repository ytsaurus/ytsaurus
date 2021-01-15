#include "handler.h"

#include "discovery_cache.h"
#include "config.h"

#include <yt/server/http_proxy/bootstrap.h>
#include <yt/server/http_proxy/coordinator.h>
#include <yt/server/http_proxy/http_authenticator.h>

#include <yt/ytlib/security_client/permission_cache.h>

#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/config.h>

#include <yt/client/api/client.h>

#include <yt/client/scheduler/operation_cache.h>

#include <yt/core/http/client.h>
#include <yt/core/http/helpers.h>

#include <yt/core/logging/log.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/cgiparam/cgiparam.h>
#include <util/string/vector.h>

#include <util/random/random.h>
#include <util/string/cast.h>

namespace NYT::NHttpProxy::NClickHouse {

using namespace NApi;
using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NYson;
using namespace NProfiling;
using namespace NLogging;
using namespace NYPath;
using namespace NTracing;
using namespace NScheduler;
using namespace NSecurityClient;

////////////////////////////////////////////////////////////////////////////////

TLogger ClickHouseLogger("ClickHouseProxy");
TRegistry ClickHouseProfiler("/clickhouse_proxy");

////////////////////////////////////////////////////////////////////////////////

class TClickHouseContext
    : public TRefCounted
{
public:
    TClickHouseContext(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TDynamicClickHouseConfigPtr& config,
        TBootstrap* bootstrap,
        const NApi::IClientPtr& client,
        const TOperationCachePtr& operationCache,
        const TPermissionCachePtr& permissionCache,
        const TDiscoveryCachePtr discoveryCache,
        const TCounter forceUpdateCounter,
        const TCounter bannedCounter,
        IInvokerPtr controlInvoker,
        NLogging::TLogger logger)
        : Logger(logger)
        , Request_(req)
        , Response_(rsp)
        , Config_(config)
        , Bootstrap_(bootstrap)
        , Client_(client)
        , HttpClient_(CreateClient(Config_->HttpClient, Bootstrap_->GetPoller()))
        , OperationCache_(operationCache)
        , PermissionCache_(permissionCache)
        , DiscoveryCache_(discoveryCache)
        , ForceUpdateCounter_(forceUpdateCounter)
        , BannedCounter_(bannedCounter)
        , ControlInvoker_(controlInvoker)
    {
        if (auto* traceParent = req->GetHeaders()->Find("traceparent")) {
            YT_LOG_INFO("Request contains traceparent header (Traceparent: %v)", traceParent);
        }

        if (auto* xRequestId = req->GetHeaders()->Find("X-Request-Id")) {
            YT_LOG_INFO("Request contains X-Request-Id header (X-Request-Id: %v)", xRequestId);
        }
    }

    bool TryPrepare()
    {
        try {
            CgiParameters_ = TCgiParameters(Request_->GetUrl().RawQuery);

            if (!TryProcessHeaders()) {
                return false;
            }

            if (!TryCheckMethod()) {
                return false;
            }

            if (!TryAuthenticate()) {
                return false;
            }

            if (!TryParseDatabase()) {
                return false;
            }

            if (!TryGetOperation()) {
                return false;
            }

            if (!TryAuthorize()) {
                return false;
            }

            bool isDatalens = false;
            // TODO(max42): remove this when DataLens makes proper authorization. Duh.
            if (auto* header = Request_->GetHeaders()->Find("X-DataLens-Real-User")) {
                YT_LOG_DEBUG("Header contains DataLens real username (RealUser: %v)", *header);
                isDatalens = true;
            }

            ProxiedRequestBody_ = Request_->ReadAll();
            if (!ProxiedRequestBody_) {
                ReplyWithError(EStatusCode::BadRequest, TError("Body should not be empty"));
                return false;
            }

            ProxiedRequestHeaders_ = Request_->GetHeaders()->Duplicate();
            ProxiedRequestHeaders_->Remove("Authorization");
            ProxiedRequestHeaders_->Add("X-Clickhouse-User", User_);
            ProxiedRequestHeaders_->Add("X-Clique-Id", ToString(OperationId_));

            CgiParameters_.EraseAll("database");
            CgiParameters_.EraseAll("query_id");
            CgiParameters_.EraseAll("span_id");

            auto* traceContext = GetCurrentTraceContext();
            YT_VERIFY(traceContext);
            traceContext->AddTag("user", User_);
            traceContext->AddTag("clique_id", ToString(OperationId_));
            if (OperationAlias_) {
                traceContext->AddTag("clique_alias", *OperationAlias_);
            }

            if (isDatalens) {
                if (auto tracingOverride = Config_->DatalensTracingOverride) {
                    traceContext->SetSampled(*tracingOverride);
                }
            } else {
                if (CgiParameters_.Has("chyt.enable_tracing")) {
                    auto enableTracing = CgiParameters_.Get("chyt.enable_tracing");
                    traceContext->SetSampled(enableTracing == "1");
                } else {
                    auto* sampler = Bootstrap_->GetCoordinator()->GetTraceSampler();
                    if (sampler->IsTraceSampled(User_)) {
                        traceContext->SetSampled(true);
                    }
                }
            }

            // COMPAT(max42): remove this, name is misleading.
            ProxiedRequestHeaders_->Add("X-Yt-Request-Id", ToString(Request_->GetRequestId()));

            auto traceIdString = ToString(traceContext->GetTraceId());
            auto spanIdString = Format("%" PRIx64, traceContext->GetSpanId());
            auto sampledString = ToString(traceContext->IsSampled());
            YT_LOG_INFO("Proxied request tracing parameters (TraceId: %v, SpanId: %v, Sampled: %v)",
                traceIdString,
                spanIdString,
                sampledString);

            ProxiedRequestHeaders_->Add("X-Yt-Trace-Id", traceIdString);
            ProxiedRequestHeaders_->Add("X-Yt-Span-Id", spanIdString);
            ProxiedRequestHeaders_->Add("X-Yt-Sampled", ToString(traceContext->IsSampled()));
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Preparation failed")
                << ex);
            return false;
        }
        return true;
    }

    bool TryPickInstance(bool forceUpdate)
    {
        YT_LOG_DEBUG("Trying to pick instance (ForceUpdate: %v)", forceUpdate);

        if (!TryDiscoverInstances(forceUpdate)) {
            YT_LOG_DEBUG("Failed to discover instances");
            return false;
        }

        if (JobCookie_.has_value()) {
            for (const auto& [id, attributes] : Instances_) {
                if (*JobCookie_ == attributes->Get<size_t>("job_cookie")) {
                    InitializeInstance(id, attributes);
                    return true;
                }
            }
        } else {
            auto instanceIterator = Instances_.begin();
            std::advance(instanceIterator, RandomNumber(Instances_.size()));
            InitializeInstance(instanceIterator->first, instanceIterator->second);
            return true;
        }
        return false;
    }

    bool TryIssueProxiedRequest(int retryIndex)
    {
        YT_LOG_DEBUG("Querying instance (Url: %v, RetryIndex: %v)", ProxiedRequestUrl_, retryIndex);

        decltype(WaitFor(HttpClient_->Post(ProxiedRequestUrl_, ProxiedRequestBody_, ProxiedRequestHeaders_))) responseOrError;
        YT_PROFILE_TIMING("/clickhouse_proxy/query_time/issue_proxied_request") {
            responseOrError = WaitFor(HttpClient_->Post(ProxiedRequestUrl_, ProxiedRequestBody_, ProxiedRequestHeaders_));
        }

        if (responseOrError.IsOK()) {
            if (responseOrError.Value()->GetStatusCode() == EStatusCode::MovedPermanently) {
                // Special status code which means that the instance is stopped by signal or clique-id in header isn't correct.
                // It is guaranteed that this instance didn't start to invoke the request, so we can retry it.
                responseOrError = TError("Instance moved, request rejected");
            } else if (!responseOrError.Value()->GetHeaders()->Find("X-ClickHouse-Server-Display-Name")) {
                // We got the response, but not from clickhouse instance.
                // Probably the instance had died and another service was started at the same host:port.
                // We can safely retry such requests.
                responseOrError = TError("The requested server is not a clickhouse instance");
            }
        }

        if (responseOrError.IsOK()) {
            ProxiedResponse_ = responseOrError.Value();
            YT_LOG_DEBUG("Got response from instance (StatusCode: %v, RetryIndex: %v)",
                ProxiedResponse_->GetStatusCode(),
                retryIndex);
            return true;
        } else {
            RequestErrors_.push_back(responseOrError
                << TErrorAttribute("instance_host", InstanceHost_)
                << TErrorAttribute("instance_http_port", InstanceHttpPort_)
                << TErrorAttribute("proxy_retry_index", retryIndex));
            YT_LOG_DEBUG(responseOrError, "Proxied request failed (RetryIndex: %v)", retryIndex);
            BannedCounter_.Increment();
            Discovery_->Ban(InstanceId_);
            return false;
        }
    }

    void ReplyWithAllOccuredErrors(TError error)
    {
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

        YT_PROFILE_TIMING("/clickhouse_proxy/query_time/forward_proxied_response") {
            WaitFor(Response_->Close())
                .ThrowOnError();
        }

        YT_LOG_DEBUG("Proxied response forwarded");
    }

    void RemoveCliqueFromCache()
    {
        Discovery_.Reset();
        DiscoveryCache_->TryRemove(OperationId_);
        YT_LOG_DEBUG("Discovery was removed from cache (OperationId: %v)", OperationId_);
    }

    const TString& GetUser() const
    {
        return User_;
    }

private:
    TLogger Logger;
    const IRequestPtr& Request_;
    const IResponseWriterPtr& Response_;
    const TDynamicClickHouseConfigPtr& Config_;
    TBootstrap* const Bootstrap_;
    const NApi::IClientPtr& Client_;
    NHttp::IClientPtr HttpClient_;
    const TOperationCachePtr OperationCache_;
    const TPermissionCachePtr PermissionCache_;
    const TDiscoveryCachePtr DiscoveryCache_;
    const TCounter ForceUpdateCounter_;
    const TCounter BannedCounter_;
    IInvokerPtr ControlInvoker_;

    // These fields contain the request details after parsing CGI params and headers.
    TCgiParameters CgiParameters_;

    TOperationIdOrAlias OperationIdOrAlias_;
    TOperationId OperationId_;
    std::optional<TString> OperationAlias_;

    TYsonString OperationAcl_;

    std::optional<size_t> JobCookie_;

    // For backward compatibility we allow GET requests when Auth is performed via token or cgi parameters.
    bool AllowGetRequests_ = false;

    // Token is provided via header "Authorization" or via cgi parameter "password".
    // If empty, the authentication will be performed via Cookie.
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

    THashMap<TString, NYTree::IAttributeDictionaryPtr> Instances_;

    void ReplyWithError(EStatusCode statusCode, const TError& error) const
    {
        YT_LOG_DEBUG(error, "Request failed (StatusCode: %v)", statusCode);
        FillYTErrorHeaders(Response_, error);
        WaitFor(Response_->WriteBody(TSharedRef::FromString(ToString(error))))
            .ThrowOnError();
    }

    void PushError(TError error)
    {
        YT_LOG_INFO(error, "Error while handling query");
        RequestErrors_.emplace_back(error);
    }

    bool ParseTokenFromAuthorizationHeader(const TString& authorization)
    {
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
                return false;
            }
        } else {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Unsupported type of authorization header (AuthorizationType: %v, TokenCount: %v)",
                    authorizationType,
                    authorizationTypeAndCredentials.size()));
            return false;
        }
        YT_LOG_DEBUG("Token parsed (AuthorizationType: %v)", authorizationType);
        return true;
    }

    bool TryProcessHeaders()
    {
        try {
            const auto* authorization = Request_->GetHeaders()->Find("Authorization");
            if (authorization && !authorization->empty()) {
                if (!ParseTokenFromAuthorizationHeader(*authorization)) {
                    return false;
                }
                if (!Token_.empty()) {
                    AllowGetRequests_ = true;
                }
            } else if (CgiParameters_.Has("password")) {
                Token_ = CgiParameters_.Get("password");
                CgiParameters_.EraseAll("password");
                CgiParameters_.EraseAll("user");
                if (!Token_.empty()) {
                    AllowGetRequests_ = true;
                }
            } else if (const auto* user = Request_->GetHeaders()->Find("X-Yt-User")) {
                if (Config_->IgnoreMissingCredentials) {
                    User_ = *user;
                }
            }
            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::BadRequest,
                TError("Error while parsing request headers")
                    << ex);
            return false;
        }
    }

    bool TryCheckMethod()
    {
        if (Request_->GetMethod() == EMethod::Post) {
            return true;
        }
        if (AllowGetRequests_ && Request_->GetMethod() == EMethod::Get) {
            return true;
        }
        Response_->SetStatus(EStatusCode::MethodNotAllowed);
        Response_->GetHeaders()->Set("Allow", "POST");
        ReplyWithError(
            EStatusCode::MethodNotAllowed,
            TError("Only POST method is allowed with your type of Authorization"));
        return false;
    }

    bool TryAuthenticate()
    {
        if (Config_->IgnoreMissingCredentials) {
            if (User_.empty()) {
                YT_LOG_DEBUG("Authentication is disabled and user was not specified; assuming root");
                User_ = "root";
            } else {
                YT_LOG_DEBUG("Authentication is disabled and user is specified via X-Yt-User header (User: %v)", User_);
            }
            return true;
        }

        try {
            YT_PROFILE_TIMING("/clickhouse_proxy/query_time/authenticate") {
                if (Token_.Empty()) {
                    User_ = Bootstrap_->GetHttpAuthenticator()->Authenticate(Request_)
                        .ValueOrThrow()
                        .Result.Login;
                } else {
                    NAuth::TTokenCredentials credentials;
                    credentials.Token = Token_;

                    User_ = WaitFor(Bootstrap_->GetTokenAuthenticator()->Authenticate(credentials))
                        .ValueOrThrow()
                        .Login;
                }
            }
            YT_LOG_DEBUG("User authenticated (User: %v)", User_);
            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Authentication failed")
                    << ex);
            return false;
        }
    }

    bool TryFindDiscovery()
    {
        if (Discovery_) {
            YT_LOG_DEBUG("Discovery is already ready");
            return true;
        }

        YT_LOG_DEBUG("Getting discovery");

        try {
            auto cookie = DiscoveryCache_->BeginInsert(OperationId_);
            if (cookie.IsActive()) {
                YT_LOG_DEBUG("Clique cache missed (CliqueId: %v)", OperationId_);

                TString path = Config_->DiscoveryPath + "/" + ToString(OperationId_);
                NApi::TGetNodeOptions options;
                options.ReadFrom = NApi::EMasterChannelKind::Cache;

                i64 version = 0;
                YT_PROFILE_TIMING("/clickhouse_proxy/query_time/create_discovery") {
                    auto nodeOrError = WaitFor(Client_->GetNode(path + "/@", options));
                    auto node = ConvertToNode(nodeOrError.ValueOrThrow())->AsMap()->FindChild("discovery_version");
                    if (node) {
                        version = node->GetValue<i64>();
                    }
                }

                YT_LOG_DEBUG("Fetched discovery version (Version: %v)", version);

                auto config = New<TDiscoveryConfig>(path);
                config->BanTimeout = Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->UnavailableInstanceBanTimeout;
                config->ReadFrom = NApi::EMasterChannelKind::Cache;
                config->MasterCacheExpireTime = Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->MasterCacheExpireTime;
                if (version == 0) {
                    config->SkipUnlockedParticipants = false;
                }

                cookie.EndInsert(New<TCachedDiscovery>(
                    OperationId_,
                    config,
                    Client_,
                    ControlInvoker_,
                    std::vector<TString>{"host", "http_port", "job_cookie"},
                    Logger));

                YT_LOG_DEBUG("New discovery inserted to the cache (OperationId: %v)", OperationId_);
            }

            YT_PROFILE_TIMING("/clickhouse_proxy/query_time/find_discovery") {
                Discovery_ = WaitFor(cookie.GetValue())
                    .ValueOrThrow();
            }

            YT_LOG_DEBUG("Discovery is ready");
        } catch (const std::exception& ex) {
            PushError(TError("Failed to create discovery")
                << ex);
            return false;
        }

        return true;
    }

    bool TryDiscoverInstances(bool forceUpdate)
    {
        YT_LOG_DEBUG("Discovering instances (ForceUpdate: %v)", forceUpdate);

        try {
            if (!TryFindDiscovery()) {
                YT_LOG_DEBUG("Failed to discover instances due to missing discovery");
                return false;
            }

            YT_LOG_DEBUG("Updating discovery (AgeThreshold: %v)", Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->SoftAgeThreshold);
            Discovery_->UpdateList(Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->SoftAgeThreshold);
            auto updatedFuture = Discovery_->UpdateList(
                forceUpdate ? Config_->ForceDiscoveryUpdateAgeThreshold : Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->HardAgeThreshold);
            if (!updatedFuture.IsSet()) {
                YT_LOG_DEBUG("Waiting for discovery");
                ForceUpdateCounter_.Increment();
                YT_PROFILE_TIMING("/clickhouse_proxy/query_time/discovery_force_update") {
                    WaitFor(updatedFuture)
                        .ThrowOnError();
                }
                YT_LOG_DEBUG("Discovery updated");
            }

            auto instances = Discovery_->List();
            YT_LOG_DEBUG("Instances discovered (Count: %v)", instances.size());
            if (instances.empty()) {
                PushError(TError("Clique %v has no running instances", OperationIdOrAlias_));
                return false;
            }

            Instances_ = instances;
            return true;
        } catch (const std::exception& ex) {
            PushError(TError("Failed to discover instances")
                << ex);
            return false;
        }
    }


    bool TryParseDatabase()
    {
        try {
            auto operationIdOrAliasAndInstanceCookie = CgiParameters_.Get("database");

            TString operationIdOrAlias;

            if (operationIdOrAliasAndInstanceCookie.Contains("@")) {
                auto separatorIndex = operationIdOrAliasAndInstanceCookie.find_last_of("@");
                operationIdOrAlias = operationIdOrAliasAndInstanceCookie.substr(0, separatorIndex);

                auto jobCookieString = operationIdOrAliasAndInstanceCookie.substr(
                    separatorIndex + 1,
                    operationIdOrAliasAndInstanceCookie.size() - separatorIndex - 1);
                size_t jobCookie = 0;
                if (!TryIntFromString<10>(jobCookieString, jobCookie)) {
                    ReplyWithError(
                        EStatusCode::BadRequest,
                        TError("Error while parsing instance cookie %v", jobCookieString));
                    return false;
                }
                JobCookie_ = jobCookie;
                YT_LOG_DEBUG("Found instance job cookie (JobCookie: %v)", *JobCookie_);
            } else {
                operationIdOrAlias = operationIdOrAliasAndInstanceCookie;
            }

            if (operationIdOrAlias.empty()) {
                ReplyWithError(
                    EStatusCode::NotFound,
                    TError("Clique id or alias should be specified using the `database` CGI parameter"));
            }

            if (operationIdOrAlias[0] == '*') {
                OperationAlias_ = operationIdOrAlias;
                YT_LOG_DEBUG("Clique is defined by alias (OperationAlias: %v)", OperationAlias_);
            } else {
                OperationId_ = TOperationId::FromString(operationIdOrAlias);
                YT_LOG_DEBUG("Clique is defined by operation id (OperationId: %v)", OperationId_);
            }

            OperationIdOrAlias_ = TOperationIdOrAlias::FromString(operationIdOrAlias);

            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::BadRequest,
                TError("Error parsing database specification")
                    << ex);
            return false;
        }
    }

    bool TryGetOperation()
    {
        try {
            auto operationYson = WaitFor(OperationCache_->Get(OperationIdOrAlias_))
                .ValueOrThrow();

            auto operationNode = ConvertTo<IMapNodePtr>(operationYson);

            if (OperationAlias_) {
                OperationId_ = ConvertTo<TOperationId>(operationNode->GetChildOrThrow("id")->GetValue<TString>());
                YT_LOG_DEBUG("Operation id resolved (OperationAlias: %v, OperationId: %v)", OperationAlias_, OperationId_);
            } else {
                YT_ASSERT(OperationId_ == ConvertTo<TOperationId>(operationNode->GetChildOrThrow("id")->GetValue<TString>()));
            }

            if (auto state = ConvertTo<EOperationState>(operationNode->GetChildOrThrow("state")->GetValue<TString>()); state != EOperationState::Running) {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Clique %v is not running; actual state = %lv", OperationIdOrAlias_, state)
                        << TErrorAttribute("operation_id", OperationId_));
                return false;
            }

            if (operationNode->GetChildOrThrow("suspended")->GetValue<bool>()) {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Clique %v is suspended; resume it to make queries", OperationIdOrAlias_));
                return false;
            }

            OperationAcl_ = ConvertToYsonString(operationNode
                ->GetChildOrThrow("runtime_parameters")
                ->AsMap()
                ->GetChildOrThrow("acl"),
                EYsonFormat::Text);

            YT_LOG_DEBUG("Operation ACL resolved (OperationAlias: %v, OperationId: %v, OperaionAcl: %v)",
                OperationAlias_,
                OperationId_,
                OperationAcl_);

            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::BadRequest,
                TError("Invalid clique specification")
                    << ex);
            return false;
        }
    }

    bool TryAuthorize()
    {
        auto error = WaitFor(PermissionCache_->Get(TPermissionKey{
                .Acl = OperationAcl_,
                .User = User_,
                .Permission = EPermission::Read
            }));

        if (!error.IsOK()) {
            ReplyWithError(
                EStatusCode::Forbidden,
                TError("User %Qv has no access to clique %v",
                    User_,
                    OperationIdOrAlias_)
                    << TErrorAttribute("operation_acl", OperationAcl_)
                    << error);
            return false;
        }

        return true;
    }

    void InitializeInstance(const TString& id, const NYTree::IAttributeDictionaryPtr& attributes)
    {
        InstanceId_ = id;
        InstanceHost_ = attributes->Get<TString>("host");
        auto port = attributes->Get<INodePtr>("http_port");
        InstanceHttpPort_ = (port->GetType() == ENodeType::String ? port->GetValue<TString>() : ToString(port->GetValue<ui64>()));

        ProxiedRequestUrl_ = Format("http://%v:%v%v?%v",
            InstanceHost_,
            InstanceHttpPort_,
            Request_->GetUrl().Path,
            CgiParameters_.Print());

        YT_LOG_DEBUG("Forwarding query to an instance (InstanceId: %v, Host: %v, HttpPort: %v, ProxiedRequestUrl: %v)",
            InstanceId_,
            InstanceHost_,
            InstanceHttpPort_,
            ProxiedRequestUrl_);
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseContext);
DECLARE_REFCOUNTED_CLASS(TClickHouseContext);

////////////////////////////////////////////////////////////////////////////////

TClickHouseHandler::TClickHouseHandler(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Coordinator_(bootstrap->GetCoordinator())
    , Config_(Bootstrap_->GetConfig()->ClickHouse)
    , Client_(Bootstrap_->GetRootClient()->GetConnection()->CreateClient(NApi::TClientOptions::FromUser(ClickHouseUserName)))
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
    , QueryCount_(ClickHouseProfiler.Counter("/query_count"))
    , ForceUpdateCount_(ClickHouseProfiler.Counter("/force_update_count"))
    , BannedCount_(ClickHouseProfiler.Counter("/banned_count"))
{
    OperationCache_ = New<TOperationCache>(
        Config_->OperationCache,
        THashSet<TString>{"id", "runtime_parameters", "state", "suspended"},
        Client_,
        ClickHouseProfiler.WithPrefix("/operation_cache"));
    PermissionCache_ = New<TPermissionCache>(
        Config_->PermissionCache,
        Bootstrap_->GetNativeConnection(),
        ClickHouseProfiler.WithPrefix("/permission_cache"));

    DiscoveryCache_ = New<TDiscoveryCache>(Config_->DiscoveryCache, ClickHouseProfiler.WithPrefix("/discovery_cache"));
}

DEFINE_ENUM(ERetryState,
    (Retrying)
    (FailedToPickInstance)
    (ForceUpdated)
    (CacheInvalidated)
    (Success)
);

void TClickHouseHandler::HandleRequest(
    const IRequestPtr& request,
    const IResponseWriterPtr& response)
{
    auto Logger = ClickHouseLogger.WithTag("RequestId: %v", request->GetRequestId());

    if (!Coordinator_->CanHandleHeavyRequests()) {
        // We intentionally read the body of the request and drop it to make sure
        // that client does not block on writing the body.
        request->ReadAll();
        RedirectToDataProxy(request, response, Coordinator_);
        return;
    }

    YT_PROFILE_TIMING("/clickhouse_proxy/total_query_time") {
        QueryCount_.Increment();
        ProcessDebugHeaders(request, response, Coordinator_);

        auto config = Bootstrap_->GetCoordinator()->GetDynamicConfig()->ClickHouse;

        if (!Bootstrap_->GetConfig()->Auth->RequireAuthentication) {
            YT_LOG_INFO("Authorization is not set up in config, ignoring missing credentials");
            config->IgnoreMissingCredentials = true;
        }

        auto context = New<TClickHouseContext>(
            request,
            response,
            config,
            Bootstrap_,
            Client_,
            OperationCache_,
            PermissionCache_,
            DiscoveryCache_,
            ForceUpdateCount_,
            BannedCount_,
            ControlInvoker_,
            Logger);

        if (!context->TryPrepare()) {
            YT_LOG_INFO("Failed to prepare context");
            return;
        }

        auto state = ERetryState::Retrying;

        auto setState = [&] (ERetryState newState) {
            YT_LOG_DEBUG("Setting new state (State: %v -> %v)", state, newState);
            state = newState;
        };

        YT_LOG_INFO("Starting retry routine (DeadInstanceRetryCount: %v)", config->DeadInstanceRetryCount);

        for (int retryIndex = 0; retryIndex <= config->DeadInstanceRetryCount; ++retryIndex) {
            YT_LOG_DEBUG("Starting new retry (RetryIndex: %v, State: %v)", retryIndex, state);
            bool needForceUpdate = false;

            if (state == ERetryState::Retrying && retryIndex > config->RetryWithoutUpdateLimit) {
                YT_LOG_DEBUG("Forcing update due to long retrying");
                needForceUpdate = true;
            } else if (state == ERetryState::FailedToPickInstance) {
                YT_LOG_DEBUG("Forcing update due to instance pick failure");
                // If we did not find any instances on previous step, we need to do force update right now.
                needForceUpdate = true;
            }

            if (needForceUpdate) {
                setState(ERetryState::ForceUpdated);
            }

            YT_LOG_DEBUG("Picking instance");
            if (!context->TryPickInstance(needForceUpdate)) {
                YT_LOG_DEBUG("Failed to pick instance (State: %v)", state);
                // There is no chance to invoke the request if we can not pick an instance even after deleting from cache.
                if (state == ERetryState::CacheInvalidated) {
                    YT_LOG_DEBUG("Stopping retrying due to failure after cache invalidation");
                    break;
                }
                // Cache may be not relevant if we can not pick an instance after discovery force update.
                if (state == ERetryState::ForceUpdated) {
                    // We may have banned all instances because of network problems or we have resolved CliqueId incorrectly
                    // (possibly due to clique restart under same alias), and cached discovery is not relevant any more.
                    YT_LOG_DEBUG("Failed to pick instance after force update, invalidating cache entry");
                    context->RemoveCliqueFromCache();
                    setState(ERetryState::CacheInvalidated);
                } else {
                    YT_LOG_DEBUG("Failed to pick instance");
                    setState(ERetryState::FailedToPickInstance);
                }

                continue;
            }

            YT_LOG_DEBUG("Pick successful, issuing proxied request");

            if (context->TryIssueProxiedRequest(retryIndex)) {
                YT_LOG_DEBUG("Successfully proxied request");
                setState(ERetryState::Success);
                break;
            }

            YT_LOG_DEBUG("Failed to proxy request (State: %v)", state);
        }

        YT_LOG_DEBUG("Finished retrying (State: %v)", state);

        if (state == ERetryState::Success) {
            ControlInvoker_->Invoke(BIND(&TClickHouseHandler::AdjustQueryCount, MakeWeak(this), context->GetUser(), +1));
            context->ForwardProxiedResponse();
            ControlInvoker_->Invoke(BIND(&TClickHouseHandler::AdjustQueryCount, MakeWeak(this), context->GetUser(), -1));
        } else {
            context->ReplyWithAllOccuredErrors(TError("Request failed"));
        }
    }
}

void TClickHouseHandler::AdjustQueryCount(const TString& user, int delta)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    auto entry = UserToRunningQueryCount_.FindOrInsert(user, [&] {
        auto gauge = ClickHouseProfiler
            .WithSparse()
            .WithTag("user", user)
            .Gauge("/running_query_count");
        return std::make_pair(0, gauge);
    }).first;

    entry->first += delta;
    entry->second.Update(entry->first);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
