#pragma once

#include "../misc/common.h"
#include "../ytree/yson_events.h"

#include <util/server/http.h>

namespace NYT {
namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////

class THttpTreeServer
{
public:
    THttpTreeServer(NYTree::TYsonProducer::TPtr ysonProducer, ui16 port);

    void Start();
    void Stop();

private:
    class TClient;
    class TCallback;

    THolder<TCallback> Callback;
    THolder<THttpServer> Server;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMonitoring
} // namespace NYT
