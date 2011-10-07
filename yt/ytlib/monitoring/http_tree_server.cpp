#include "http_tree_server.h"

#include "../ytree/json_adapter.h"

namespace NYT {
namespace NMonitoring {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer::TClient
    : public ::TClientRequest
{
public:
    TClient(TYsonProducer::TPtr ysonProducer)
        : YsonProducer(ysonProducer)
    { }

    virtual bool Reply(void* /*ThreadSpecificResource*/)
    {
        if (strnicmp(~Headers[0], "GET ", 4)) {
            Output() << "HTTP/1.0 501 Not Implemented\r\n\r\n";
            return true;
        } 

        Output() <<
            "HTTP/1.0 200 OK\r\n"
            "Content-Type: text/plain\r\n"
            "\r\n";

        TJsonAdapter adapter(&Output());
        YsonProducer->Do(&adapter);

        Output() << "\r\n";

        return true;
    }


private:
    TYsonProducer::TPtr YsonProducer;
};

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer::TCallback
    : public THttpServer::ICallBack
{
public:
    TCallback(TYsonProducer::TPtr ysonProducer)
        : YsonProducer(ysonProducer)
    { }

    virtual TClientRequest* CreateClient()
    {
        return new TClient(YsonProducer);
    }
    
private:
    TYsonProducer::TPtr YsonProducer;
};

////////////////////////////////////////////////////////////////////////////////

THttpTreeServer::THttpTreeServer(TYsonProducer::TPtr ysonProducer, int port)
    : Callback(new TCallback(ysonProducer))
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
