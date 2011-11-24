#include "stdafx.h"
#include "http_tree_server.h"

#include "../ytree/json_adapter.h"
#include "../ytree/ypath_rpc.h"
#include "../ytree/yson_reader.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer::TClient
    : public ::TClientRequest
{
public:
    TClient(const THandlerMap& handlers)
        : Handlers(handlers)
    { }

    virtual bool Reply(void* /*ThreadSpecificResource*/)
    {
        auto tokens = splitStroku(~Headers[0], " ");
        if (tokens[0] != "GET") {
            Output() << "HTTP/1.0 501 Not Implemented\r\n\r\n";
            return true;
        }
        auto path = tokens[1];

        FOREACH (auto pair, Handlers) {
            auto prefix = pair.First();
            if (path.has_prefix(prefix)) {
                auto suffix = path.substr(prefix.length());
                auto handler = pair.Second();
                auto future = handler->Do(suffix);
                auto result = future->Get();
                Output() << result;

                return true;
            }
        }
        return false;
    }


private:
    const THandlerMap& Handlers;
};

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer::TCallback
    : public THttpServer::ICallBack
{
public:
    TCallback(const THandlerMap& handlers)
        : Handlers(handlers)
    { }

    virtual TClientRequest* CreateClient()
    {
        return new TClient(Handlers);
    }
    
private:
    const THandlerMap& Handlers;
};

////////////////////////////////////////////////////////////////////////////////

THttpTreeServer::THttpTreeServer(int port)
    : Callback(new TCallback(Handlers))
{
    Server.Reset(new THttpServer(
        ~Callback,
        THttpServerOptions(static_cast<ui16>(port))));
}

void THttpTreeServer::Start()
{
    Server->Start();
}

void THttpTreeServer::Stop()
{
    Server->Stop();
}

void THttpTreeServer::Register(const Stroka& prefix, THandler::TPtr handler)
{
    YVERIFY(Handlers.insert(MakePair(prefix, handler)).Second());
}

////////////////////////////////////////////////////////////////////////////////

using namespace NYTree;

namespace {

Stroka OnResponse(TYPathProxy::TRspGet::TPtr response)
{
    if (!response->IsOK()) {
        return
            "HTTP/1.1 500 Internal Server Error\r\n"
            "Content-Type: text/plain\r\n"            "\r\n"
            + response->GetError().ToString();
    }
    TStringStream output;
    output <<
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "\r\n";
    TJsonAdapter adapter(&output);
    TYsonReader ysonReader(&adapter);
    TStringStream ysonStream;
    ysonStream << response->GetValue();
    ysonReader.Read(&ysonStream);
    adapter.Flush();
    return output.Str();
}

TFuture<Stroka>::TPtr AsyncGet(IYPathService::TPtr pathService, TYPath path)
{
    if (~pathService == NULL) {
        return ToFuture(
            Stroka(
                "HTTP/1.1 503 YPath Service Unavailable\r\n"
                "Content-Type: text/plain\r\n"                "\r\n"));
    }
    auto request = TYPathProxy::Get();
    request->SetPath(path);
    auto response = ExecuteVerb(~pathService, ~request);
    return response->Apply(FromMethod(&OnResponse));
}

TFuture<Stroka>::TPtr YTreeHandler(
    Stroka path,
    TYPathServiceAsyncProvider::TPtr asyncProvider)
{
    return
        asyncProvider
        ->Do()
        ->Apply(FromMethod(&AsyncGet, path));
}

} // namespace

THttpTreeServer::THandler::TPtr GetYPathServiceHandler(
    TYPathServiceAsyncProvider::TPtr pathServiceAsyncProvider)
{
    return FromMethod(&YTreeHandler, pathServiceAsyncProvider);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
