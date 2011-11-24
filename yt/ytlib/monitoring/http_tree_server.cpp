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
        if (strnicmp(~Headers[0], "GET ", 4) != 0) {
            Output() << "HTTP/1.0 501 Not Implemented\r\n\r\n";
            return true;
        }
        auto path = Headers[0].substr(4);

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
    TStringStream output;
    output <<
        "HTTP/1.0 200 OK\r\n"
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

TFuture<Stroka>::TPtr YTreeHandler(Stroka path, IYPathService::TPtr pathService)
{
    auto request = TYPathProxy::Get();
    request->SetPath(path);
    auto response = ExecuteVerb(~pathService, ~request);
    return response->Apply(FromMethod(&OnResponse));
}

} // namespace

THttpTreeServer::THandler::TPtr GetYPathServiceHandler(IYPathService* pathService)
{
    return FromMethod(&YTreeHandler, IYPathService::TPtr(pathService));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
