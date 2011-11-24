#pragma once

#include "../misc/common.h"
#include "../ytree/ypath_service.h"

#include <util/server/http.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer
    : public TNonCopyable
{
public:
    typedef IParamFunc<Stroka, TFuture<Stroka>::TPtr> THandler;

    THttpTreeServer(int port);

    void Register(const Stroka& prefix, THandler::TPtr handler);
    void Start();
    void Stop();

private:
    class TClient;
    class TCallback;

    typedef yhash_map<Stroka, THandler::TPtr> THandlerMap;

    THandlerMap Handlers;
    THolder<TCallback> Callback;
    THolder<THttpServer> Server;
};

////////////////////////////////////////////////////////////////////////////////

THttpTreeServer::THandler::TPtr GetYPathServiceHandler(
    NYTree::IYPathService* pathService);

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
