#pragma once

#include "yson_events.h"

#include <util/server/http.h>

namespace NYT {
namespace NYTree {

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer
{
public:
    THttpTreeServer(TYsonProducer::TPtr ysonProducer, ui16 port);

    void Start();
    void Stop();

private:
    class TClient;
    class TCallback;

    THolder<TCallback> Callback;
    THolder<THttpServer> Server;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYTree
} // namespace NYT
