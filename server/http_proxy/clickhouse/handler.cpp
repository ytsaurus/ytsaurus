#include "handler.h"

#include "clique_cache.h"
#include "config.h"

#include <yt/server/http_proxy/bootstrap.h>
#include <yt/server/http_proxy/coordinator.h>

#include <yt/ytlib/auth/token_authenticator.h>
#include <yt/ytlib/auth/config.h>

#include <yt/client/api/client.h>

#include <yt/core/http/client.h>
#include <yt/core/http/helpers.h>

#include <yt/core/logging/log.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/profiling/profile_manager.h>

#include <library/string_utils/base64/base64.h>

#include <library/cgiparam/cgiparam.h>
#include <util/string/vector.h>

#include <util/random/random.h>
#include <util/string/cast.h>

namespace NYT::NHttpProxy::NClickHouse {

using namespace NApi;
using namespace NConcurrency;
using namespace NHttp;
using namespace NYTree;
using namespace NProfiling;
using namespace NLogging;
using namespace NYPath;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

TLogger ClickHouseLogger("ClickHouseProxy");
TProfiler ClickHouseProfiler("/clickhouse_proxy");

// It is needed for PROFILE_AGGREGATED_TIMING macros.
static const auto& Profiler = ClickHouseProfiler;

////////////////////////////////////////////////////////////////////////////////

class TClickHouseContext
    : public TIntrinsicRefCounted
{
public:
    TClickHouseContext(
        const IRequestPtr& req,
        const IResponseWriterPtr& rsp,
        const TClickHouseConfigPtr& config,
        TBootstrap* bootstrap,
        const NHttp::IClientPtr& httpClient,
        const NApi::IClientPtr& client,
        const TCliqueCachePtr cliqueCache,
        IInvokerPtr controlInvoker,
        TClickHouseHandler::TClickHouseProxyMetrics& metrics)
        : Logger(TLogger(ClickHouseLogger).AddTag("RequestId: %v", req->GetRequestId()))
        , Request_(req)
        , Response_(rsp)
        , Config_(config)
        , Bootstrap_(bootstrap)
        , Client_(client)
        , HttpClient_(httpClient)
        , CliqueCache_(cliqueCache)
        , ControlInvoker_(controlInvoker)
        , Metrics_(metrics)
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

            if (!TryAuthenticate()) {
                return false;
            }

            auto cliqueAndInstanceCookie = CgiParameters_.Get("database");

            if (cliqueAndInstanceCookie.Contains("@")) {
                auto separatorIndex = cliqueAndInstanceCookie.find_last_of("@");
                CliqueIdOrAlias_ = cliqueAndInstanceCookie.substr(0, separatorIndex);

                auto jobCookieString = cliqueAndInstanceCookie.substr(
                    separatorIndex + 1, cliqueAndInstanceCookie.size() - separatorIndex - 1);
                size_t jobCookie = 0;
                if (!TryIntFromString<10>(jobCookieString, jobCookie)) {
                    ReplyWithError(
                        EStatusCode::BadRequest,
                        TError("Error while parsing job cookie %v", jobCookieString));
                    return false;
                }
                JobCookie_ = jobCookie;
                YT_LOG_DEBUG("Found instance job cookie (JobCookie: %v)", *JobCookie_);
            } else {
                CliqueIdOrAlias_ = cliqueAndInstanceCookie;
            }

            if (CliqueIdOrAlias_.empty()) {
                ReplyWithError(
                    EStatusCode::NotFound,
                    TError("Clique id or alias should be specified using the `database` CGI parameter"));
            }

            YT_LOG_DEBUG("Clique id parsed (CliqueId: %v)", CliqueIdOrAlias_);

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

            CgiParameters_.EraseAll("database");
            CgiParameters_.EraseAll("query_id");
            CgiParameters_.EraseAll("span_id");

            auto* traceContext = GetCurrentTraceContext();
            YT_VERIFY(traceContext);

            if (isDatalens) {
                if (auto tracingOverride = Bootstrap_->GetCoordinator()->GetDynamicConfig()->DatalensTracingOverride) {
                    traceContext->SetSampled(*tracingOverride);
                }
            } else {
                // For non-datalens queries, force sampling.
                traceContext->SetSampled(true);
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
        if (!TryDiscoverInstances(forceUpdate)) {
            return false;
        }

        if (JobCookie_.has_value()) {
            for (const auto& [id, attributes] : Instances_) {
                if (*JobCookie_ == attributes.at("job_cookie")->GetValue<size_t>()) {
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
        PROFILE_AGGREGATED_TIMING(Metrics_.IssueProxiedRequestTime) {
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
            Discovery_->Ban(InstanceId_);
            ClickHouseProfiler.Increment(Metrics_.BannedCount);
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

        PROFILE_AGGREGATED_TIMING(Metrics_.ForwardProxiedResponseTime) {
            WaitFor(Response_->Close())
                .ThrowOnError();
        }

        YT_LOG_DEBUG("Proxied response forwarded");
    }

    void RemoveCliqueFromCache()
    {
        Discovery_.Reset();
        CliqueId_.reset();
        CliqueCache_->TryRemove(CliqueIdOrAlias_);
        YT_LOG_DEBUG("Discovery was removed from cache (CliqueIdOrAlias: %v)", CliqueIdOrAlias_);
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
    TBootstrap* const Bootstrap_;
    const NApi::IClientPtr& Client_;
    const NHttp::IClientPtr& HttpClient_;
    const TCliqueCachePtr CliqueCache_;
    IInvokerPtr ControlInvoker_;

    // These fields contain the request details after parsing CGI params and headers.
    TCgiParameters CgiParameters_;
    // CliqueId or alias.

    TString CliqueIdOrAlias_;
    // Do TryResolveAlias() to set up this values.
    // Will be set automatically after finding Discovery in cache.
    std::optional<TString> CliqueId_;
    std::optional<size_t> JobCookie_;

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

    TClickHouseHandler::TClickHouseProxyMetrics& Metrics_;

    THashMap<TString, NYTree::TAttributeMap> Instances_;

    void SetCliqueId(TString cliqueId)
    {
        CliqueId_ = std::move(cliqueId);
        ProxiedRequestHeaders_->Set("X-Clique-Id", CliqueId_.value());
    }

    void ReplyWithError(EStatusCode statusCode, const TError& error) const
    {
        YT_LOG_DEBUG(error, "Request failed (StatusCode: %v)", statusCode);
        FillYTErrorHeaders(Response_, error);
        WaitFor(Response_->WriteBody(TSharedRef::FromString(ToString(error))))
            .ThrowOnError();
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
            TGuid operationId;
            PROFILE_AGGREGATED_TIMING(Metrics_.ResolveAliasTime) {
                operationId = ConvertTo<TGuid>(WaitFor(
                    Client_->GetNode(
                        Format("//sys/scheduler/orchid/scheduler/operations/%v/operation_id",
                            ToYPathLiteral(CliqueIdOrAlias_)),
                        options))
                        .ValueOrThrow());
            }
            SetCliqueId(ToString(operationId));
            YT_LOG_DEBUG("Alias resolved (Alias: %v, CliqueId: %v)", CliqueIdOrAlias_, CliqueId_);
            return true;
        } catch (const std::exception& ex) {
            RequestErrors_.emplace_back(TError("Error while resolving alias %Qv", CliqueIdOrAlias_)
                << ex);
            return false;
        }
    }

    void ParseTokenFromAuthorizationHeader(const TString& authorization)
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

            PROFILE_AGGREGATED_TIMING(Metrics_.AuthenticateTime) {
                User_ = WaitFor(Bootstrap_->GetTokenAuthenticator()->Authenticate(credentials))
                    .ValueOrThrow()
                    .Login;
            }

            YT_LOG_DEBUG("User authenticated (User: %v)", User_);
            return true;
        } catch (const std::exception& ex) {
            ReplyWithError(
                EStatusCode::Unauthorized,
                TError("Authorization failed")
                    << ex);
            return false;
        }
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

                i64 version = 0;
                PROFILE_AGGREGATED_TIMING(Metrics_.CreateDiscoveryTime) {
                    auto nodeOrError = WaitFor(Client_->GetNode(path + "/@", options));
                    auto node = ConvertToNode(nodeOrError.ValueOrThrow())->AsMap()->FindChild("discovery_version");
                    if (node) {
                        version = node->GetValue<i64>();
                    }
                }

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
                    std::vector<TString>{"host", "http_port", "job_cookie"},
                    Logger));
            }

            PROFILE_AGGREGATED_TIMING(Metrics_.FindDiscoveryTime) {
                Discovery_ = WaitFor(cookie.GetValue())
                    .ValueOrThrow();
            }

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

    bool TryDiscoverInstances(bool forceUpdate)
    {
        try {
            if (!TryFindDiscovery()) {
                return false;
            }

            Discovery_->UpdateList(Config_->CliqueCache->SoftAgeThreshold);
            auto updatedFuture = Discovery_->UpdateList(
                forceUpdate ? Config_->ForceDiscoveryUpdateAgeThreshold : Config_->CliqueCache->HardAgeThreshold);
            if (!updatedFuture.IsSet()) {
                ClickHouseProfiler.Increment(Metrics_.ForceUpdateCount);
                PROFILE_AGGREGATED_TIMING(Metrics_.DiscoveryForceUpdateTime) {
                    WaitFor(updatedFuture)
                        .ThrowOnError();
                }
            }

            auto instances = Discovery_->List();
            if (instances.empty()) {
                RequestErrors_.emplace_back("Clique %v has no running instances", CliqueIdOrAlias_);
                return false;
            }

            Instances_ = instances;
            return true;
        } catch (const std::exception& ex) {
            RequestErrors_.push_back(TError("Failed to get discovery instances")
                << ex);
            return false;
        }
    }

    void InitializeInstance(const TString& id, const NYTree::TAttributeMap& attributes)
    {
        InstanceId_ = id;
        InstanceHost_ = attributes.at("host")->GetValue<TString>();
        auto port = attributes.at("http_port");
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
    , HttpClient_(CreateClient(Config_->HttpClient, Bootstrap_->GetPoller()))
    , Client_(Bootstrap_->GetRootClient()->GetConnection()->CreateClient(NApi::TClientOptions(ClickHouseUserName)))
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
{
    if (!Bootstrap_->GetConfig()->Auth->RequireAuthentication) {
        Config_->IgnoreMissingCredentials = true;
    }
    if (Bootstrap_->GetConfig()->ClickHouse->ForceEnqueueProfiling) {
        ClickHouseProfiler.ForceEnqueue() = true;
    }
    CliqueCache_ = New<TCliqueCache>(Config_->CliqueCache, ClickHouseProfiler.AppendPath("/clique_cache"));

    ProfilingExecutor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TClickHouseHandler::OnProfiling, MakeWeak(this)),
        Config_->ProfilingPeriod);
    ProfilingExecutor_->Start();
}

DEFINE_ENUM(ERetryState,
    (Retrying)
    (FailedToPickInstance)
    (ForceUpdated)
    (CacheRemoved)
    (Success)
);

void TClickHouseHandler::HandleRequest(
    const IRequestPtr& request,
    const IResponseWriterPtr& response)
{
    if (!Coordinator_->CanHandleHeavyRequests()) {
        // We intentionally read the body of the request and drop it to make sure
        // that client does not block on writing the body.
        request->ReadAll();
        RedirectToDataProxy(request, response, Coordinator_);
    } else PROFILE_AGGREGATED_TIMING(Metrics_.TotalQueryTime) {
        ClickHouseProfiler.Increment(Metrics_.QueryCount);
        ProcessDebugHeaders(request, response, Coordinator_);
        auto context = New<TClickHouseContext>(
            request,
            response,
            Config_,
            Bootstrap_,
            HttpClient_,
            Client_,
            CliqueCache_,
            ControlInvoker_,
            Metrics_);

        if (!context->TryPrepare()) {
            return;
        }

        const auto& Logger = ClickHouseLogger;

        auto state = ERetryState::Retrying;

        for (int retry = 0; retry <= Config_->DeadInstanceRetryCount; ++retry) {
            YT_LOG_DEBUG("Starting new retry (RetryIndex: %v, State: %v)", retry, state);
            bool needForceUpdate = false;

            if (state == ERetryState::Retrying && retry > Config_->RetryWithoutUpdateLimit) {
                needForceUpdate = true;
            } else if (state == ERetryState::FailedToPickInstance) {
                // If we did not find any instances on previous step, we need to do force update right now.
                needForceUpdate = true;
            }

            if (needForceUpdate) {
                state = ERetryState::ForceUpdated;
            }

            if (!context->TryPickInstance(needForceUpdate)) {
                // There is no chance to invoke the request if we can not pick an instance even after deleting from cache.
                if (state == ERetryState::CacheRemoved) {
                    break;
                }
                // Cache may be not relevant if we can not pick an instance after discovery force update.
                if (state == ERetryState::ForceUpdated) {
                    // We may have banned all instances because of network problems or we have resolved CliqueId incorrectly
                    // (possibly due to clique restart under same alias), and cached discovery is not relevant any more.
                    context->RemoveCliqueFromCache();
                    state = ERetryState::CacheRemoved;
                } else {
                    state = ERetryState::FailedToPickInstance;
                }

                continue;
            }

            if (context->TryIssueProxiedRequest(retry)) {
                state = ERetryState::Success;
                break;
            }
        }

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
