#include "handler.h"

#include "config.h"
#include "discovery_cache.h"

#include <yt/yt/server/http_proxy/bootstrap.h>
#include <yt/yt/server/http_proxy/coordinator.h>
#include <yt/yt/server/http_proxy/http_authenticator.h>

#include <yt/yt/ytlib/api/native/config.h>
#include <yt/yt/ytlib/api/native/connection.h>

#include <yt/yt/ytlib/scheduler/helpers.h>

#include <yt/yt/ytlib/security_client/permission_cache.h>

#include <yt/yt/library/auth_server/config.h>
#include <yt/yt/library/auth_server/helpers.h>
#include <yt/yt/library/auth_server/token_authenticator.h>

#include <yt/yt/library/clickhouse_discovery/discovery_v1.h>
#include <yt/yt/library/clickhouse_discovery/discovery_v2.h>
#include <yt/yt/library/clickhouse_discovery/helpers.h>

#include <yt/yt/client/api/client.h>

#include <yt/yt/client/scheduler/operation_cache.h>

#include <yt/yt/core/http/client.h>
#include <yt/yt/core/http/helpers.h>

#include <yt/yt/core/logging/fluent_log.h>
#include <yt/yt/core/logging/log.h>

#include <yt/yt/core/concurrency/periodic_executor.h>

#include <yt/yt/core/rpc/bus/channel.h>
#include <yt/yt/core/rpc/caching_channel_factory.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <library/cpp/cgiparam/cgiparam.h>

#include <util/string/cast.h>
#include <util/string/vector.h>

#include <util/random/random.h>

namespace NYT::NHttpProxy::NClickHouse {

using namespace NApi;
using namespace NClickHouseServer;
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
using namespace NRpc::NBus;
using namespace NDiscoveryClient;

////////////////////////////////////////////////////////////////////////////////

TLogger ClickHouseUnstructuredLogger("ClickHouseProxy");
TLogger ClickHouseStructuredLogger("ClickHouseProxyStructured");
TProfiler ClickHouseProfiler("/clickhouse_proxy");

////////////////////////////////////////////////////////////////////////////////

namespace {

DEFINE_ENUM(ERetryState,
    (Retrying)
    (FailedToPickInstance)
    (ForceUpdated)
    (CacheInvalidated)
    (Success)
);

} // namespace

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
        const TClickHouseHandlerPtr& handler,
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
        , Handler_(handler)
        , ChannelFactory_(CreateTcpBusChannelFactory(New<NBus::TBusConfig>()))
    {
        if (auto* traceParent = req->GetHeaders()->Find("traceparent")) {
            YT_LOG_INFO("Request contains traceparent header (Traceparent: %v)", traceParent);
        }

        if (auto* xRequestId = req->GetHeaders()->Find("X-Request-Id")) {
            YT_LOG_INFO("Request contains X-Request-Id header (X-Request-Id: %v)", xRequestId);
        }
    }

    void ProcessRequest()
    {
        if (!TryPrepare()) {
            YT_LOG_INFO(ResponseError_, "Failed to prepare context");
            return;
        }

        if (!TryForwardRequest()) {
            YT_LOG_INFO(ResponseError_, "Failed to forward request");
            return;
        }

        if (!TryForwardProxiedResponse()) {
            YT_LOG_INFO(ResponseError_, "Failed to forward proxied response");
            return;
        }
    }

    void LogStructuredRequest()
    {
        LogStructuredEventFluently(ClickHouseStructuredLogger, ELogLevel::Info)
            .Item("request_id").Value(Request_->GetRequestId())
            .OptionalItem("authenticated_user", !User_.Empty() ? std::make_optional(User_) : std::nullopt)
            .OptionalItem("token_hash", !Token_.Empty() ? std::make_optional(NAuth::GetCryptoHash(Token_)) : std::nullopt)
            .Item("proxy_address").Value(Bootstrap_->GetCoordinator()->GetSelf()->GetHost())
            .Item("client_address").Value(ToString(Request_->GetRemoteAddress()))
            .OptionalItem("user_agent", FindUserAgent(Request_))
            .Item("http_method").Value(Request_->GetMethod())
            .Item("is_https").Value(Request_->IsHttps())

            .Item("in_bytes").Value(Request_->GetReadByteCount())
            .Item("out_bytes").Value(Response_->GetWriteByteCount())

            .OptionalItem("trace_id", TryGetCurrentTraceContext() ?
                std::make_optional(GetCurrentTraceContext()->GetTraceId()) :
                std::nullopt)
            .OptionalItem("query_id", ProxiedResponse_ && ProxiedResponse_->GetHeaders()->Find("X-ClickHouse-Query-Id") ?
                std::make_optional(ProxiedResponse_->GetHeaders()->GetOrThrow("X-ClickHouse-Query-Id")) :
                std::nullopt)
            .OptionalItem("clique_alias", CliqueAlias_)
            .OptionalItem("clique_id", OperationId_ ? std::make_optional(OperationId_) : std::nullopt)
            .OptionalItem("coordinator_id", !InstanceId_.Empty() ? std::make_optional(InstanceId_) : std::nullopt)
            .OptionalItem("coordinator_address", !InstanceHost_.Empty() ? std::make_optional(InstanceHost_) : std::nullopt)
            .OptionalItem("proxied_request_url", !ProxiedRequestUrl_.Empty() ?
                std::make_optional(ProxiedRequestUrl_) :
                std::nullopt)
            .OptionalItem("retry_count", RetryCount_ >= 0 ? std::make_optional(RetryCount_) : std::nullopt)

            .Item("http_code").Value(static_cast<int>(Response_->GetStatus().value_or(EStatusCode::OK)))
            .Item("start_time").Value(Request_->GetStartTime())
            .Item("duration").Value(static_cast<i64>((TInstant::Now() - Request_->GetStartTime()).MicroSeconds()))
            .OptionalItem("error_code", !ResponseError_.IsOK() ?
                std::make_optional(static_cast<int>(ResponseError_.GetCode())) :
                std::nullopt)
            .OptionalItem("error", !ResponseError_.IsOK() ? std::make_optional(ResponseError_) : std::nullopt)

            .OptionalItem("datalens_real_user", FindHeader(Request_, "X-DataLens-Real-User"))
            .OptionalItem("x_request_id", FindHeader(Request_, "X-Request-Id"))
            .OptionalItem("yql_operation_id", FindHeader(Request_, "X-YQL-Operation-ID"));
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

    TOperationId OperationId_;
    TString CliqueAlias_;

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
    TClickHouseHandlerPtr Handler_;
    NRpc::IChannelFactoryPtr ChannelFactory_;

    static const inline std::vector<TString> DiscoveryAttributes_ = std::vector<TString>{
        "host",
        "http_port",
        "job_cookie",
        "clique_incarnation",
    };

    // These fields define the proxied request issued to a randomly chosen instance.
    TString ProxiedRequestUrl_;
    TSharedRef ProxiedRequestBody_;
    THeadersPtr ProxiedRequestHeaders_;

    //! Response from a chosen instance.
    IResponsePtr ProxiedResponse_;

    std::vector<TError> RequestErrors_;
    TError ResponseError_;

    THashMap<TString, NYTree::IAttributeDictionaryPtr> Instances_;

    // Fields for structured log only.
    int RetryCount_ = -1;
    TString TokenHash_;

    void ReplyWithError(EStatusCode statusCode, const TError& error)
    {
        YT_LOG_DEBUG(error, "Request failed (StatusCode: %v)", statusCode);
        ResponseError_ = error;

        FillYTErrorHeaders(Response_, error);
        Response_->SetStatus(statusCode);

        // TODO(dakovalkov): Do not throw error here.
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

            if (!TryCheckMethod()) {
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
            // User authentication is done on proxy only. We do not need to send the token to the clique.
            // Instead, we send the authenticated username via "X-Clickhouse-User" header.
            ProxiedRequestHeaders_->Remove("Authorization");
            ProxiedRequestHeaders_->Remove("X-Clickhouse-Key");

            ProxiedRequestHeaders_->Set("X-Clickhouse-User", User_);
            // In rare cases the request can be sent to an instance from another clique.
            // Setting 'expected clique id' helps to detect these situations and reject the request.
            ProxiedRequestHeaders_->Set("X-Clique-Id", ToString(OperationId_));
            // Status '100 Continue' is not handled properly in our HttpClient.
            // Remove 'Expect' header to prevent such response status.
            ProxiedRequestHeaders_->Remove("Expect");

            CgiParameters_.EraseAll("database");
            CgiParameters_.EraseAll("query_id");
            CgiParameters_.EraseAll("span_id");
            CgiParameters_.EraseAll("user");
            CgiParameters_.EraseAll("password");

            auto* traceContext = TryGetCurrentTraceContext();
            YT_VERIFY(traceContext);
            traceContext->AddTag("user", User_);
            traceContext->AddTag("clique_id", OperationId_);
            traceContext->AddTag("clique_alias", CliqueAlias_);

            if (isDatalens) {
                if (auto tracingOverride = Config_->DatalensTracingOverride) {
                    traceContext->SetSampled(*tracingOverride);
                }
            } else {
                if (CgiParameters_.Has("chyt.enable_tracing")) {
                    auto enableTracing = CgiParameters_.Get("chyt.enable_tracing");
                    traceContext->SetSampled(enableTracing == "1");
                } else {
                    Bootstrap_->GetCoordinator()->GetTraceSampler()->SampleTraceContext(User_, traceContext);
                }
            }

            // COMPAT(max42): remove this, name is misleading.
            ProxiedRequestHeaders_->Set("X-Yt-Request-Id", ToString(Request_->GetRequestId()));

            auto traceIdString = ToString(traceContext->GetTraceId());
            auto spanIdString = Format("%" PRIx64, traceContext->GetSpanId());
            auto sampledString = ToString(traceContext->IsSampled());
            YT_LOG_INFO("Proxied request tracing parameters (TraceId: %v, SpanId: %v, Sampled: %v)",
                traceIdString,
                spanIdString,
                sampledString);

            ProxiedRequestHeaders_->Set("X-Yt-Trace-Id", traceIdString);
            ProxiedRequestHeaders_->Set("X-Yt-Span-Id", spanIdString);
            ProxiedRequestHeaders_->Set("X-Yt-Sampled", ToString(traceContext->IsSampled()));
        } catch (const std::exception& ex) {
            ReplyWithError(EStatusCode::InternalServerError, TError("Preparation failed")
                << ex);
            return false;
        }
        return true;
    }

    bool TryForwardRequest()
    {
        auto state = ERetryState::Retrying;

        auto setState = [&] (ERetryState newState) {
            YT_LOG_DEBUG("Setting new state (State: %v -> %v)", state, newState);
            state = newState;
        };

        YT_LOG_INFO("Starting retry routine (DeadInstanceRetryCount: %v)", Config_->DeadInstanceRetryCount);

        for (int retryIndex = 0; retryIndex <= Config_->DeadInstanceRetryCount; ++retryIndex) {
            YT_LOG_DEBUG("Starting new retry (RetryIndex: %v, State: %v)", retryIndex, state);
            bool needForceUpdate = false;

            if (state == ERetryState::Retrying && retryIndex > Config_->RetryWithoutUpdateLimit) {
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

            YT_LOG_DEBUG("Picking instance (RetryIndex: %v)", retryIndex);
            if (!TryPickInstance(needForceUpdate)) {
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
                    RemoveCliqueFromCache();
                    setState(ERetryState::CacheInvalidated);
                } else {
                    YT_LOG_DEBUG("Failed to pick instance (RetryIndex: %v)", retryIndex);
                    setState(ERetryState::FailedToPickInstance);
                }

                continue;
            }

            YT_LOG_DEBUG("Pick successful, issuing proxied request (RetryIndex: %v)", retryIndex);

            if (TryIssueProxiedRequest(retryIndex)) {
                YT_LOG_DEBUG("Successfully proxied request (RetryIndex: %v)", retryIndex);
                setState(ERetryState::Success);
                break;
            } else {
                YT_LOG_DEBUG("Failed to proxy request (RetryIndex: %v, State: %v)", retryIndex, state);
            }
        }

        YT_LOG_DEBUG("Finished retrying (State: %v)", state);

        if (state != ERetryState::Success) {
            ReplyWithAllOccurredErrors(TError("Request failed"));
            return false;
        }

        return true;
    }

    bool TryForwardProxiedResponse()
    {
        YT_LOG_DEBUG("Getting proxied status code");
        auto statusCode = ProxiedResponse_->GetStatusCode();
        Response_->SetStatus(statusCode);
        YT_LOG_DEBUG("Received status code, getting proxied headers (StatusCode: %v)", statusCode);
        Response_->GetHeaders()->MergeFrom(ProxiedResponse_->GetHeaders());
        YT_LOG_DEBUG("Received headers, forwarding proxied response");
        PipeInputToOutput(ProxiedResponse_, Response_);

        YT_PROFILE_TIMING("/clickhouse_proxy/query_time/forward_proxied_response") {
            if (auto error = WaitFor(Response_->Close()); !error.IsOK()) {
                YT_LOG_DEBUG(error, "Failed to forward proxied response");
                // The connection could be already closed, so we can not reply with error.
                // But we save the error for a proper ClickHouseStructuredLog's entry anyway.
                ResponseError_ = std::move(error);
                return false;
            }
        }

        YT_LOG_DEBUG("Proxied response forwarded");
        return true;
    }

    bool TryProcessHeaders()
    {
        try {
            const auto* authorization = Request_->GetHeaders()->Find("Authorization");
            const auto* xClickHouseKey = Request_->GetHeaders()->Find("X-ClickHouse-Key");

            if (authorization && !authorization->empty()) {
                if (!ParseTokenFromAuthorizationHeader(*authorization)) {
                    return false;
                }
                if (!Token_.empty()) {
                    AllowGetRequests_ = true;
                }
            } else if (xClickHouseKey && !xClickHouseKey->empty()) {
                // store header value as-is
                Token_ = *xClickHouseKey;
                if (!Token_.empty()) {
                    AllowGetRequests_ = true;
                }
            } else if (CgiParameters_.Has("password")) {
                Token_ = CgiParameters_.Get("password");
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
                if (!Token_.empty() && Bootstrap_->GetConfig()->ClickHouse->PopulateUserWithToken) {
                    YT_LOG_DEBUG("Authentication is disabled and user is specified via token's value");
                    User_ = Token_;
                } else {
                    YT_LOG_DEBUG("Authentication is disabled and user was not specified; assuming root");
                    User_ = "root";
                }
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

                    auto authenticator = Bootstrap_->GetHttpAuthenticator()->GetTokenAuthenticatorOrThrow(Request_->GetPort());
                    User_ = WaitFor(authenticator->Authenticate(credentials))
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

    IDiscoveryPtr CreateDiscoveryV1()
    {
        auto config = New<TDiscoveryV1Config>();
        auto path = Format("%v/%v", Config_->DiscoveryPath, OperationId_);
        config->Directory = path;
        config->BanTimeout = Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->UnavailableInstanceBanTimeout;
        config->ReadFrom = NApi::EMasterChannelKind::Cache;
        config->MasterCacheExpireTime = Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->MasterCacheExpireTime;
        return NClickHouseServer::CreateDiscoveryV1(
            std::move(config),
            Client_,
            ControlInvoker_,
            DiscoveryAttributes_,
            Logger);
    }

    TString GetOperationAlias() const
    {
        return "*" + CliqueAlias_;
    }

    TString GetDiscoveryGroupId() const
    {
        return "/chyt/" + CliqueAlias_;
    }

    IDiscoveryPtr CreateDiscoveryV2()
    {
        auto config = New<TDiscoveryV2Config>();
        config->GroupId = GetDiscoveryGroupId();
        config->ReadQuorum = 1;
        config->WriteQuorum = 1;
        config->BanTimeout = Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->UnavailableInstanceBanTimeout;
        return NClickHouseServer::CreateDiscoveryV2(
            std::move(config),
            Bootstrap_->GetNativeConnection(),
            ChannelFactory_,
            ControlInvoker_,
            DiscoveryAttributes_,
            Logger);
    }

    IDiscoveryPtr TryChooseDiscovery()
    {
        auto discoveryV1 = CreateDiscoveryV1();
        auto discoveryV1Future = discoveryV1->UpdateList().Apply(BIND([discovery = std::move(discoveryV1)] { return discovery; }));
        auto futures = std::vector{discoveryV1Future};

        if (Bootstrap_->GetNativeConnection()->GetConfig()->DiscoveryConnection) {
            auto discoveryV2 = CreateDiscoveryV2();
            auto discoveryV2Future = discoveryV2->UpdateList().Apply(BIND([discovery = std::move(discoveryV2)] { return discovery; }));
            futures.emplace_back(std::move(discoveryV2Future));
        } else {
            YT_LOG_DEBUG("Skipping discovery v2 because of missing discovery connection config (ClusterConnection: %v)",
                ConvertToYsonString(Bootstrap_->GetNativeConnection()->GetConfig(), EYsonFormat::Text).ToString());
        }

        auto valueOrError = WaitFor(AnySucceeded(futures));
        if (!valueOrError.IsOK()) {
            if (valueOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                THROW_ERROR_EXCEPTION("Clique directory does not exist; perhaps the clique is still starting, wait for up to 5 minutes")
                    << valueOrError;
            } else {
                THROW_ERROR_EXCEPTION("Clique discovery is not found")
                    << valueOrError;
            }
        }

        return valueOrError.Value();
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
                YT_LOG_DEBUG("Clique cache missed (Clique: %v)", CliqueAlias_);

                auto discovery = TryChooseDiscovery();

                YT_LOG_DEBUG("Fetched discovery version (Version: %v)", discovery->Version());

                cookie.EndInsert(New<TCachedDiscovery>(
                    OperationId_,
                    std::move(discovery)));

                YT_LOG_DEBUG("New discovery inserted to the cache (Clique: %v)", CliqueAlias_);
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
            YT_UNUSED_FUTURE(Discovery_->Value()->UpdateList(Bootstrap_->GetConfig()->ClickHouse->DiscoveryCache->SoftAgeThreshold));
            auto updatedFuture = Discovery_->Value()->UpdateList(
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

            auto instances = Discovery_->Value()->List();
            if (Discovery_->Value()->Version() == 2) {
                instances = FilterInstancesByIncarnation(instances);
            }
            YT_LOG_DEBUG("Instances discovered (Count: %v)", instances.size());
            if (instances.empty()) {
                PushError(TError("Clique %v has no running instances", CliqueAlias_));
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
            auto cliqueAliasAndInstanceCookie = CgiParameters_.Get("database");

            TString cliqueAlias;

            if (cliqueAliasAndInstanceCookie.Contains("@")) {
                auto separatorIndex = cliqueAliasAndInstanceCookie.find_last_of("@");
                cliqueAlias = cliqueAliasAndInstanceCookie.substr(0, separatorIndex);

                auto jobCookieString = cliqueAliasAndInstanceCookie.substr(
                    separatorIndex + 1,
                    cliqueAliasAndInstanceCookie.size() - separatorIndex - 1);
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
                cliqueAlias = cliqueAliasAndInstanceCookie;
            }

            if (cliqueAlias.StartsWith("*")) {
                cliqueAlias.erase(0, 1);
                YT_LOG_DEBUG("Strip asterisk from clique alias provided by user (%v)", cliqueAlias);
            }

            if (cliqueAlias.empty()) {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Clique alias should be specified using the `database` CGI parameter"));
                return false;
            }

            CliqueAlias_ = std::move(cliqueAlias);
            YT_LOG_DEBUG("Clique is defined by alias (%v)", CliqueAlias_);

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
        auto operationId = Handler_->GetOperationId(CliqueAlias_);
        if (operationId) {
            OperationId_ = operationId;
            YT_LOG_DEBUG("Operation resolved from strawberry nodes (OperationAlias: %v, OperationId: %v)",
                GetOperationAlias(),
                OperationId_);
            return true;
        }

        YT_LOG_DEBUG("Clique information in Cypress is malformed, operation id is missing (Clique: %v)",
            CliqueAlias_);

        YT_LOG_DEBUG("Fetching operation from scheduler (Clique: %v)", CliqueAlias_);

        try {
            // Operation-cache wants alias to start with asterisk
            auto operationYson = WaitFor(OperationCache_->Get(GetOperationAlias()))
                .ValueOrThrow();

            auto operationNode = ConvertTo<IMapNodePtr>(operationYson);

            OperationId_ = operationNode->GetChildValueOrThrow<TOperationId>("id");
            YT_LOG_DEBUG("Operation id resolved (OperationAlias: %v, OperationId: %v)", GetOperationAlias(), OperationId_);

            if (auto state = operationNode->GetChildValueOrThrow<EOperationState>("state"); state != EOperationState::Running) {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Clique %v is not running; actual state = %lv", CliqueAlias_, state)
                        << TErrorAttribute("operation_id", OperationId_));
                return false;
            }

            if (operationNode->GetChildValueOrThrow<bool>("suspended")) {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Clique %v is suspended; resume it to make queries", CliqueAlias_));
                return false;
            }

            OperationAcl_ = ConvertToYsonString(operationNode
                ->GetChildOrThrow("runtime_parameters")
                ->AsMap()
                ->GetChildOrThrow("acl"),
                EYsonFormat::Text);

            YT_LOG_DEBUG("Operation ACL resolved (OperationAlias: %v, OperationId: %v, OperationAcl: %v)",
                GetOperationAlias(),
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
        TFuture<void> future;
        if (OperationAcl_) {
            future = PermissionCache_->Get(TPermissionKey{
                .Acl = OperationAcl_,
                .User = User_,
                .Permission = EPermission::Read,
            });
        } else {
            future = PermissionCache_->Get(TPermissionKey{
                .Object = Format("//sys/access_control_object_namespaces/chyt/%v/principal", CliqueAlias_),
                .User = User_,
                .Permission = EPermission::Use,
            });
        }

        auto error = WaitFor(future);
        if (!error.IsOK()) {
            if (error.FindMatching(NSecurityClient::EErrorCode::AuthorizationError)) {
                auto replyError = TError("User %Qv has no access to clique %v",
                    User_,
                    CliqueAlias_)
                    << error;
                if (OperationAcl_) {
                    replyError.MutableAttributes()->Set("operation_acl", OperationAcl_);
                }
                ReplyWithError(EStatusCode::Forbidden, replyError);
            } else {
                ReplyWithError(
                    EStatusCode::BadRequest,
                    TError("Failed to authorize user %Qv to clique %v",
                        User_,
                        CliqueAlias_)
                        << error);
            }
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
        // Save retry count for structured log.
        RetryCount_ = retryIndex;

        YT_LOG_DEBUG("Querying instance (Url: %v, RetryIndex: %v)", ProxiedRequestUrl_, retryIndex);

        TErrorOr<IResponsePtr> responseOrError;
        YT_PROFILE_TIMING("/clickhouse_proxy/query_time/issue_proxied_request") {
            responseOrError = WaitFor(HttpClient_->Post(ProxiedRequestUrl_, ProxiedRequestBody_, ProxiedRequestHeaders_));
        }

        if (responseOrError.IsOK()) {
            auto response = responseOrError.Value();

            if (response->GetStatusCode() == EStatusCode::MovedPermanently) {
                // Special status code which means that the instance is stopped by signal or clique-id in header isn't correct.
                // It is guaranteed that this instance didn't start to invoke the request, so we can retry it.
                responseOrError = TError("Instance moved, request rejected");
            } else if (!response->GetHeaders()->Find("X-ClickHouse-Server-Display-Name")) {
                // We got the response, but not from clickhouse instance.
                // Probably the instance had died and another service was started at the same host:port.
                // We can safely retry such requests.

                THashSet<TString> headers;
                for (const auto& [header, value] : response->GetHeaders()->Dump()) {
                    headers.emplace(header);
                }

                auto statusCode = response->GetStatusCode();
                auto statusCodeStr = ToString(static_cast<int>(statusCode)) + " (" + ToString(statusCode)+ ")";

                responseOrError = TError("The requested server is not a clickhouse instance")
                    << TErrorAttribute("status_code", statusCodeStr)
                    << TErrorAttribute("headers", headers);
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
            Discovery_->Value()->Ban(InstanceId_);
            return false;
        }
    }

    void ReplyWithAllOccurredErrors(TError error)
    {
        ReplyWithError(EStatusCode::InternalServerError, error
            << RequestErrors_);
    }

    void InitializeInstance(const TString& id, const NYTree::IAttributeDictionaryPtr& attributes)
    {
        InstanceId_ = id;
        InstanceHost_ = attributes->Get<TString>("host");
        auto port = attributes->Get<INodePtr>("http_port");
        InstanceHttpPort_ = (port->GetType() == ENodeType::String ? port->AsString()->GetValue() : ToString(port->GetValue<ui64>()));

        ProxiedRequestUrl_ = Format("http://%v:%v%v?%v",
            InstanceHost_,
            InstanceHttpPort_,
            "/", // ClickHouse implements different endpoints but we use only default one
            CgiParameters_.Print());

        YT_LOG_DEBUG("Forwarding query to an instance (InstanceId: %v, Host: %v, HttpPort: %v, ProxiedRequestUrl: %v)",
            InstanceId_,
            InstanceHost_,
            InstanceHttpPort_,
            ProxiedRequestUrl_);
    }

    void RemoveCliqueFromCache()
    {
        Discovery_.Reset();
        DiscoveryCache_->TryRemove(OperationId_);
        YT_LOG_DEBUG("Discovery was removed from cache (Clique: %v)", CliqueAlias_);
    }
};

DEFINE_REFCOUNTED_TYPE(TClickHouseContext)
DECLARE_REFCOUNTED_CLASS(TClickHouseContext)

////////////////////////////////////////////////////////////////////////////////

TClickHouseHandler::TClickHouseHandler(TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , Coordinator_(Bootstrap_->GetCoordinator())
    , Config_(Bootstrap_->GetConfig()->ClickHouse)
    , Client_(Bootstrap_->GetRootClient()->GetConnection()->CreateClient(NApi::TClientOptions::FromUser(ClickHouseUserName)))
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
    , QueryCount_(ClickHouseProfiler.Counter("/query_count"))
    , ForceUpdateCount_(ClickHouseProfiler.Counter("/force_update_count"))
    , BannedCount_(ClickHouseProfiler.Counter("/banned_count"))
    , OperationIdUpdateExecutor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TClickHouseHandler::UpdateOperationIds, MakeWeak(this)),
        Config_->OperationIdUpdatePeriod))
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

void TClickHouseHandler::Start()
{
    OperationIdUpdateExecutor_->Start();
}

void TClickHouseHandler::HandleRequest(
    const IRequestPtr& request,
    const IResponseWriterPtr& response)
{
    auto Logger = ClickHouseUnstructuredLogger
        .WithTag("RequestId: %v", request->GetRequestId());

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

        auto config = Bootstrap_->GetDynamicConfig()->ClickHouse;

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
            MakeStrong(this),
            ControlInvoker_,
            Logger);

        auto adjuctQueryCountCallback = BIND(&TClickHouseHandler::AdjustQueryCount, MakeWeak(this), context->GetUser());
        ControlInvoker_->Invoke(BIND(adjuctQueryCountCallback, +1));
        auto queryCountGuard = Finally(BIND(adjuctQueryCountCallback, -1).Via(ControlInvoker_));

        try {
            context->ProcessRequest();
        } catch (const std::exception& ex) {
            YT_LOG_INFO(ex, "Request failed with unexpected error");
        }

        context->LogStructuredRequest();
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
        return std::pair(0, gauge);
    }).first;

    entry->first += delta;
    entry->second.Update(entry->first);
}

void TClickHouseHandler::UpdateOperationIds()
{
    auto Logger = ClickHouseUnstructuredLogger;
    THashMap<TString, TOperationId> aliasToOperationId;
    try {
        TListNodeOptions options;
        options.ReadFrom = EMasterChannelKind::MasterCache;
        options.Attributes = {"strawberry_persistent_state"};
        auto listResult = WaitFor(Client_->ListNode(Config_->ChytStrawberryPath, options))
            .ValueOrThrow();
        auto strawberryNodes = ConvertTo<std::vector<IStringNodePtr>>(listResult);
        for (const auto& node : strawberryNodes) {
            auto alias = node->GetValue();
            try {
                auto strawberryPersistentState = node->Attributes()
                    .Get<IMapNodePtr>("strawberry_persistent_state");
                auto ytOperationState = strawberryPersistentState->GetChildValueOrThrow<EOperationState>("yt_operation_state");
                if (!IsOperationFinished(ytOperationState)) {
                    auto operationId = strawberryPersistentState->GetChildValueOrThrow<TOperationId>("yt_operation_id");
                    aliasToOperationId[alias] = operationId;
                }
            } catch (const std::exception& ex) {
                YT_LOG_DEBUG(ex, "Cannot extract operation id from strawberry node (Clique: %v)", alias);
                continue;
            }
        }
    } catch (const std::exception& ex) {
        // Non-throwing method for periodic executor
        YT_LOG_DEBUG(ex, "Cannot update operation information map");
    }

    if (!aliasToOperationId.empty()) {
        auto guard = WriterGuard(OperationIdLock_);
        AliasToOperationId_.swap(aliasToOperationId);
    }

    AliasToOperationIdInitialized_ = true;
}

TOperationId TClickHouseHandler::GetOperationId(const TString& alias) const
{
    if (!AliasToOperationIdInitialized_) {
        auto future = OperationIdUpdateExecutor_->GetExecutedEvent();
        OperationIdUpdateExecutor_->ScheduleOutOfBand();
        WaitForFast(future).ThrowOnError();
    }

    auto guard = ReaderGuard(OperationIdLock_);
    auto it = AliasToOperationId_.find(alias);
    return (it != AliasToOperationId_.end()) ? it->second : TOperationId();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttpProxy::NClickHouse
