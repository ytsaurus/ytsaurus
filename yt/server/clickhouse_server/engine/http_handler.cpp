#include "http_handler.h"

//#include <server/HTTPHandler.h>
//#include <server/NotFoundHandler.h>
//#include <server/PingRequestHandler.h>
//#include <server/RootRequestHandler.h>

//#include <common/logger_useful.h>

//#include <Poco/Net/HTTPServerRequest.h>
//#include <Poco/URI.h>

namespace NYT {
namespace NClickHouseServer {
namespace NEngine {

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

class THttpHandlerFactory
    : public Poco::Net::HTTPRequestHandlerFactory
{
private:
    IServer& Server;
    Poco::Logger* Log;

public:
    THttpHandlerFactory(IServer& server)
        : Server(server)
        , Log(&Logger::get("HTTPHandlerFactory"))
    {}

    Poco::Net::HTTPRequestHandler* createRequestHandler(
        const Poco::Net::HTTPServerRequest& request) override;
};

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandler* THttpHandlerFactory::createRequestHandler(
    const Poco::Net::HTTPServerRequest& request)
{
    const Poco::URI uri(request.getURI());

    CH_LOG_INFO(Log, "HTTP Request. "
        << "Method: " << request.getMethod()
        << ", URI: " << uri.toString()
        << ", Address: " << request.clientAddress().toString()
        << ", User-Agent: " << (request.has("User-Agent") ? request.get("User-Agent") : "none"));

    // Light health-checking requests
    if (IsHead(request) || IsGet(request)) {
        if (uri == "/") {
            return new RootRequestHandler(Server);
        }
        if (uri == "/ping") {
            return new PingRequestHandler(Server);
        }
    }

    // Query execution
    // HTTPHandler executes query in read-only mode for GET requests
    if (IsGet(request) || IsPost(request)) {
        if ((uri.getPath() == "/") ||
            (uri.getPath() == "/query")) {
            return new HTTPHandler(Server);
        }
    }

    return new NotFoundHandler();
}

////////////////////////////////////////////////////////////////////////////////

Poco::Net::HTTPRequestHandlerFactory::Ptr CreateHttpHandlerFactory(IServer& server)
{
    return new THttpHandlerFactory(server);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NEngine
} // namespace NClickHouseServer
} // namespace NYT
