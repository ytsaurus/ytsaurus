#include "http_handler.h"

#include "query_context.h"
#include "host.h"
#include "helpers.h"

#include <yt/yt/library/re2/re2.h>

#include <Poco/Util/LayeredConfiguration.h>
#include <Server/HTTPHandler.h>
#include <Server/NotFoundHandler.h>
#include <Server/StaticRequestHandler.h>

#include <Access/AccessControl.h>
#include <Access/User.h>

#include <Poco/URI.h>

#include <base/getFQDNOrHostName.h>

#include <util/string/cast.h>

namespace NYT::NClickHouseServer {

using namespace NTracing;
using namespace NLogging;

////////////////////////////////////////////////////////////////////////////////

class TMovedPermanentlyRequestHandler
    : public DB::HTTPRequestHandler
{
public:
    TMovedPermanentlyRequestHandler(DB::IServer& server)
        : Server_(server)
    { }

    void handleRequest(
        DB::HTTPServerRequest& /*request*/,
        DB::HTTPServerResponse& response) override
    {
        try {
            response.set("X-ClickHouse-Server-Display-Name", Server_.config().getString("display_name", getFQDNOrHostName()));
            response.setStatusAndReason(DB::HTTPResponse::HTTP_MOVED_PERMANENTLY);
            (*response.send()) << "Instance moved or is moving from this address.\n";
        } catch (...) {
            DB::tryLogCurrentException("MovedPermanentlyHandler");
        }
    }

private:
    DB::IServer& Server_;
};

class THttpHandler
    : public DB::DynamicQueryHandler
{
public:
    THttpHandler(THost* host, DB::IServer& server, const DB::HTTPServerRequest& request)
        : DB::DynamicQueryHandler(server, "handler")
        , Host_(host)
        , Server_(server)
    {
        TraceContext_ = SetupTraceContext(ClickHouseYtLogger, request);
        TraceContext_->AddTag("chyt.instance_cookie", Host_->GetInstanceCookie());
        TraceContext_->AddTag("chyt.instance_address", Host_->GetConfig()->Address);

        // By default, trace id coincides with query id. It makes significantly easier to
        // debug singular queries like those which are issued via YQL.
        // If trace id is external (i.e. suggested via traceparent header), we cannot use
        // trace id as a query id as it will result in several queries sharing same query id.
        // So, we form query id as a mix of trace id lower part and a span id.
        if (request.get("traceparent", "").empty()) {
            QueryId_ = TraceContext_->GetTraceId();
        } else {
            // If trace id = 11111111-22222222-33333333-44444444 and span id = 5555555566666666,
            // then query id will be 33333333-44444444-55555555-66666666.
            QueryId_.Parts64[1] = TraceContext_->GetTraceId().Parts64[0];
            QueryId_.Parts64[0] = TraceContext_->GetSpanId();
        }
    }

    void customizeContext(DB::HTTPServerRequest& /*request*/, DB::ContextMutablePtr context, DB::ReadBuffer& /*body*/) override
    {
        YT_VERIFY(TraceContext_);

        // For HTTP queries (which are always initial) query id is same as trace id.

        auto queryId = ToString(QueryId_);
        context->setInitialQueryId(queryId);
        context->setCurrentQueryId(queryId);

        SetupHostContext(Host_, context, QueryId_, TraceContext_, DataLensRequestId_, YqlOperationId_);
    }

    void handleRequest(DB::HTTPServerRequest& request, DB::HTTPServerResponse& response) override
    {
        const auto& Logger = ClickHouseYtLogger;

        response.set("X-Yt-Trace-Id", ToString(TraceContext_->GetTraceId()));

        auto replyError = [&] (Poco::Net::HTTPResponse::HTTPStatus statusCode, const TError& error) {
            YT_LOG_INFO(error, "Replying with error");
            // Without this header proxy thinks that the response is not from clickhouse instance.
            response.set("X-ClickHouse-Server-Display-Name", Server_.config().getString("display_name", getFQDNOrHostName()));
            response.setStatusAndReason(statusCode);
            (*response.send()) << ToString(error);
        };
        auto userName = request.get("X-ClickHouse-User", "");
        const auto& userNameBlacklist = Host_->GetConfig()->UserNameBlacklist;
        const auto& userNameWhitelist = Host_->GetConfig()->UserNameWhitelist;
        if (userName.empty()) {
            replyError(DB::HTTPResponse::HTTP_UNAUTHORIZED, TError("User name should be specified via X-ClickHouse-User header"));
            return;
        } else if (userNameBlacklist && NRe2::TRe2::FullMatch(userName, *userNameBlacklist) &&
                   (!userNameWhitelist || !NRe2::TRe2::FullMatch(userName, *userNameWhitelist))) {
            replyError(DB::HTTPResponse::HTTP_FORBIDDEN, TError("User name %Qv is banned by blacklist regular expression %Qv", userName, userNameBlacklist->pattern()));
            return;
        }

        auto userAgent = request.get("User-Agent", "");
        const auto& userAgentBlacklist = Host_->GetConfig()->UserAgentBlacklist;

        if (userAgentBlacklist.contains(userAgent)) {
            replyError(DB::HTTPResponse::HTTP_FORBIDDEN, TError("User Agent %Qv is banned in clique config by blacklist %Qv", userAgent, userAgentBlacklist));
            return;
        }

        if (TryDiscardQueryDueToSampling(userAgent)) {
            replyError(
                DB::HTTPResponse::HTTP_BAD_REQUEST,
                TError(
                    "Discarding query due to query sampling with rate %v",
                    Host_->GetConfig()->QuerySampling->QuerySamplingRate));
            return;
        }

        YT_LOG_DEBUG("Registering new user (UserName: %v)", userName);
        RegisterNewUser(
            Server_.context()->getAccessControl(),
            TString(userName),
            Host_->HasUserDefinedSqlObjectStorage());
        YT_LOG_DEBUG("User registered");

        DB::HTTPHandler::handleRequest(request, response);
    }

private:
    THost* const Host_;
    DB::IServer& Server_;
    TTraceContextPtr TraceContext_;
    std::optional<TString> DataLensRequestId_;
    TQueryId QueryId_;
    std::optional<TString> YqlOperationId_;

    //! If span is present in query headers, parse it and setup trace context which is its child.
    //! Otherwise, generate our own trace id (aka query id) and maybe generate root trace context
    //! if X-Yt-Sampled = true.
    TTraceContextPtr SetupTraceContext(
        const TLogger& logger,
        const DB::HTTPServerRequest& request)
    {
        const auto& Logger = logger;

        auto maybeDataLensRequestId = request.get("X-Request-Id", "");
        if (maybeDataLensRequestId.starts_with("dl.")) {
            YT_LOG_INFO("Request contains DataLens request id (RequestId: %v)", maybeDataLensRequestId);
            DataLensRequestId_ = TString(maybeDataLensRequestId);
        }

        auto maybeYqlOperationId = request.get("X-YQL-Operation-Id", "");
        if (!maybeYqlOperationId.empty()) {
            YT_LOG_INFO("Request contains YQL operation id (OperationId: %v)", maybeYqlOperationId);
            YqlOperationId_ = TString(maybeYqlOperationId.substr(0, YqlOperationIdLength));
        }

        TSpanContext parentSpan;
        auto requestTraceId = request.get("X-Yt-Trace-Id", "");
        auto requestSpanId = request.get("X-Yt-Span-Id", "");
        if (!TTraceId::FromString(requestTraceId, &parentSpan.TraceId) ||
            !TryIntFromString<16>(requestSpanId, parentSpan.SpanId))
        {
            parentSpan = TSpanContext{
                .TraceId = TTraceId::Create()
            };
            YT_LOG_INFO(
                "Parent span context is absent or not parseable, generating our own trace id aka query id "
                "(RequestTraceId: %v, RequestSpanId: %v, GeneratedTraceId: %v)",
                requestTraceId,
                requestSpanId,
                parentSpan.TraceId);
        } else {
            YT_LOG_INFO("Parsed parent span context (RequestTraceId: %v, RequestSpanId: %v)",
                requestTraceId,
                requestSpanId);
        }

        auto requestSampled = TString(request.get("X-Yt-Sampled", ""));
        if (int intValue; TryIntFromString<10>(requestSampled, intValue) && intValue >= 0 && intValue <= 1) {
            YT_LOG_INFO("Parsed X-Yt-Sampled (RequestSampled: %v)",
                requestSampled);
            parentSpan.Sampled = (intValue == 1);
        } else if (bool boolValue; TryFromString<bool>(requestSampled, boolValue)) {
            YT_LOG_INFO("Parsed X-Yt-Sampled (RequestSampled: %v)",
                requestSampled);
            parentSpan.Sampled = boolValue;
        } else {
            YT_LOG_INFO("Cannot parse X-Yt-Sampled, assuming false (RequestSampled: %v)",
                requestSampled);
            parentSpan.Sampled = false;
        }

        return New<TTraceContext>(parentSpan, "HttpHandler");
    }

    bool TryDiscardQueryDueToSampling(const std::string& userAgent)
    {
        auto querySamplingConfig = Host_->GetConfig()->QuerySampling;
        auto userAgentRegExp = querySamplingConfig->UserAgentRegExp;
        if (userAgentRegExp && NRe2::TRe2::FullMatch(userAgent, *userAgentRegExp)) {
            return THash<TQueryId>()(QueryId_) > querySamplingConfig->QuerySamplingRate * std::numeric_limits<size_t>::max();
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class THttpHandlerFactory
    : public DB::HTTPRequestHandlerFactory
{
public:
    THttpHandlerFactory(THost* host, DB::IServer& server)
        : Host_(host)
        , Server_(server)
    { }

    std::unique_ptr<DB::HTTPRequestHandler> createRequestHandler(const DB::HTTPServerRequest& request) override
    {
        Poco::URI uri(request.getURI());

        const auto& Logger = ClickHouseYtLogger;
        YT_LOG_INFO("HTTP request received (Method: %v, URI: %v, Address: %v, UserAgent: %v)",
            request.getMethod(),
            uri.toString(),
            request.clientAddress().toString(),
            (request.has("User-Agent") ? request.get("User-Agent") : "none"));

        // Light health-checking requests.
        if (request.getMethod() == DB::HTTPServerRequest::HTTP_HEAD ||
            request.getMethod() == DB::HTTPServerRequest::HTTP_GET)
        {
            if (uri == "/" || uri == "/ping") {
                return std::make_unique<DB::StaticRequestHandler>(Server_, "Ok.\n");
            }
        }

        auto cliqueId = request.find("X-Clique-Id");
        if (Host_->GetInstanceState() == EInstanceState::Stopped ||
            (cliqueId != request.end() && TString(cliqueId->second) != ToString(Host_->GetConfig()->CliqueId)))
        {
            return std::make_unique<TMovedPermanentlyRequestHandler>(Server_);
        }

        if (request.getMethod() == DB::HTTPServerRequest::HTTP_GET ||
            request.getMethod() == DB::HTTPServerRequest::HTTP_POST)
        {
            if ((uri.getPath() == "/") ||
                (uri.getPath() == "/query")) {
                auto handler = std::make_unique<THttpHandler>(Host_, Server_, request);
                return handler;
            }
        }

        return std::make_unique<DB::NotFoundHandler>(std::vector<std::string>{});
    }

private:
    THost* Host_;
    DB::IServer& Server_;
};

////////////////////////////////////////////////////////////////////////////////

DB::HTTPRequestHandlerFactoryPtr CreateHttpHandlerFactory(THost* host, DB::IServer& server)
{
    return std::make_shared<THttpHandlerFactory>(host, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
