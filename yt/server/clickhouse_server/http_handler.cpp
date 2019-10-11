#include "http_handler.h"

#include "query_context.h"

#include <server/HTTPHandler.h>
#include <server/NotFoundHandler.h>
#include <server/PingRequestHandler.h>
#include <server/RootRequestHandler.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>

#include <yt/core/misc/string.h>

#include <util/string/cast.h>

namespace NYT::NClickHouseServer {

using namespace DB;
using namespace NTracing;

////////////////////////////////////////////////////////////////////////////////

namespace {

bool IsHead(const Poco::Net::HTTPServerRequest& request)
{
    return request.getMethod() == Poco::Net::HTTPRequest::HTTP_HEAD;
}

bool IsGet(const Poco::Net::HTTPServerRequest& request)
{
    return request.getMethod() == Poco::Net::HTTPRequest::HTTP_GET;
}

bool IsPost(const Poco::Net::HTTPServerRequest& request)
{
    return request.getMethod() == Poco::Net::HTTPRequest::HTTP_POST;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class MovedPermanentlyRequestHandler : public Poco::Net::HTTPRequestHandler
{
public:
    void handleRequest(
        Poco::Net::HTTPServerRequest & /* request */,
        Poco::Net::HTTPServerResponse & response) override
    {
        try {
            response.setStatusAndReason(Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY);
            response.send() << "Instance moved or is moving from this address.\n";
        } catch (...) {
            tryLogCurrentException("MovedPermanentlyHandler");
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class THttpHandlerFactory
    : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    TBootstrap* Bootstrap_;
    IServer& Server;

public:
    THttpHandlerFactory(TBootstrap* bootstrap, IServer& server)
        : Bootstrap_(bootstrap)
        , Server(server)
    {}

    Poco::Net::HTTPRequestHandler* createRequestHandler(
        const Poco::Net::HTTPServerRequest& request) override;
};

////////////////////////////////////////////////////////////////////////////////

//! If span is present in query headers, parse it and setup trace context which is its child.
//! Otherwise, generate our own trace id (aka query id) and maybe generate root trace context
//! if X-Yt-Sampled = true.
TTraceContextPtr SetupTraceContext(
    const TLogger& logger,
    const Poco::Net::HTTPServerRequest& request)
{
    const auto& Logger = logger;

    TSpanContext parentSpan;
    auto requestTraceId = request.get("X-Yt-Trace-Id", "");
    auto requestSpanId = request.get("X-Yt-Span-Id", "");
    if (!TTraceId::FromString(requestTraceId, &parentSpan.TraceId) ||
        !TryIntFromString<16>(requestSpanId, parentSpan.SpanId))
    {
        parentSpan = TSpanContext{TTraceId::Create(), InvalidSpanId, false, false};
        YT_LOG_INFO(
            "Parent span context is absent or not parseable, generating our own trace id aka query id "
            "(RequestTraceId: %Qv, RequestSpanId: %Qv, GeneratedTraceId: %v)",
            requestTraceId,
            requestSpanId,
            parentSpan.TraceId);
    } else {
        YT_LOG_INFO("Parsed parent span context (RequestTraceId: %Qv, RequestSpanId: %Qv)",
            requestTraceId,
            requestSpanId);
    }

    auto requestSampled = request.get("X-Yt-Sampled", "");
    int sampled;
    if (!TryIntFromString<10>(TString(requestSampled), sampled) || sampled < 0 || sampled > 1) {
        YT_LOG_INFO("Cannot parse X-Yt-Sampled, assuming false (RequestSampled: %Qv)", requestSampled);
        sampled = 0;
    } else {
        YT_LOG_INFO("Parsed X-Yt-Sampled (RequetSampled: %Qv)", requestSampled);
    }

    auto traceContext = New<TTraceContext>(parentSpan, "HttpHandler");
    if (sampled == 1) {
        traceContext->SetSampled();
    }

    return traceContext;
}

Poco::Net::HTTPRequestHandler* THttpHandlerFactory::createRequestHandler(
    const Poco::Net::HTTPServerRequest& request)
{
    class THttpHandler
        : public DB::HTTPHandler
    {
    public:
        THttpHandler(TBootstrap* bootstrap, DB::IServer& server, TTraceContextPtr traceContext)
            : DB::HTTPHandler(server)
            , Bootstrap_(bootstrap)
            , TraceContext_(std::move(traceContext))
        { }

        virtual void customizeContext(DB::Context& context) override
        {
            YT_VERIFY(TraceContext_);
            // For HTTP queries (which are always initial) query id is same as trace id.
            context.getClientInfo().current_query_id = context.getClientInfo().initial_query_id = ToString(TraceContext_->GetTraceId());
            SetupHostContext(Bootstrap_, context, TraceContext_->GetTraceId(), std::move(TraceContext_));
        }

    private:
        TBootstrap* const Bootstrap_;
        TTraceContextPtr TraceContext_;
    };

    Poco::URI uri(request.getURI());

    const auto& Logger = ServerLogger;
    YT_LOG_INFO("HTTP request received (Method: %v, URI: %v, Address: %v, UserAgent: %v)",
        request.getMethod(),
        uri.toString(),
        request.clientAddress().toString(),
        (request.has("User-Agent") ? request.get("User-Agent") : "none"));

    // Light health-checking requests
    if (IsHead(request) || IsGet(request)) {
        if (uri == "/") {
            return new RootRequestHandler(Server);
        }
        if (uri == "/ping") {
            return new PingRequestHandler(Server);
        }
    }

    auto cliqueId = request.find("X-Clique-Id");
    if (Bootstrap_->GetState() == EInstanceState::Stopped ||
        (cliqueId != request.end() && TString(cliqueId->second) != Bootstrap_->GetCliqueId()))
    {
        return new MovedPermanentlyRequestHandler();
    }

    auto traceContext = SetupTraceContext(Logger, request);

    if (IsGet(request) || IsPost(request)) {
        if ((uri.getPath() == "/") ||
            (uri.getPath() == "/query")) {
            auto* handler = new THttpHandler(Bootstrap_, Server, std::move(traceContext));
            return handler;
        }
    }

    return new NotFoundHandler();
}

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(TBootstrap* bootstrap, IServer& server)
{
    return new THttpHandlerFactory(bootstrap, server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClickHouseServer
