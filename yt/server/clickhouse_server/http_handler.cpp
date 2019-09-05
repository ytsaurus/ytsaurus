#include "http_handler.h"

#include "query_context.h"

#include <server/HTTPHandler.h>
#include <server/NotFoundHandler.h>
#include <server/PingRequestHandler.h>
#include <server/RootRequestHandler.h>

#include <Poco/Net/HTTPServerRequest.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/URI.h>

namespace NYT::NClickHouseServer {

using namespace DB;

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

Poco::Net::HTTPRequestHandler* THttpHandlerFactory::createRequestHandler(
    const Poco::Net::HTTPServerRequest& request)
{
    class THttpHandler
        : public DB::HTTPHandler
    {
    public:
        THttpHandler(TBootstrap* bootstrap, DB::IServer& server, TQueryId queryId)
            : DB::HTTPHandler(server)
            , Bootstrap_(bootstrap)
            , QueryId_(queryId)
        { }

        virtual void customizeContext(DB::Context& context) override
        {
            SetupHostContext(Bootstrap_, context, QueryId_);
        }

    private:
        TBootstrap* const Bootstrap_;
        const TQueryId QueryId_;
    };

    Poco::URI uri(request.getURI());

    const auto& Logger = ServerLogger;
    YT_LOG_DEBUG("HTTP request received (Method: %v, URI: %v, Address: %v, User-Agent: %v)",
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

    auto ytRequestId = request.get("X-Yt-Request-Id", "");
    TQueryId queryId;
    if (!TQueryId::FromString(ytRequestId, &queryId)) {
        queryId = TQueryId::Create();
    }

    // Query execution
    // HTTPHandler executes query in read-only mode for GET requests
    if (IsGet(request) || IsPost(request)) {
        if ((uri.getPath() == "/") ||
            (uri.getPath() == "/query")) {
            auto* handler = new THttpHandler(Bootstrap_, Server, queryId);
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
