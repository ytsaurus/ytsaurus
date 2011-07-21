#pragma once

#include "common.h"
#include "service.h"
#include "../bus/bus_server.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IMessageHandler
{
public:
    typedef TIntrusivePtr<TServer> TPtr;

    TServer(int port);

    void RegisterService(IService::TPtr service);
    void Start();

    Stroka GetDebugInfo();

private:
    typedef yhash_map<Stroka, IService::TPtr> TServiceMap;

    TBusServer::TPtr BusServer;
    TServiceMap Services;
    volatile bool Started;

    IService::TPtr GetService(Stroka serviceName);
    virtual void OnMessage(IMessage::TPtr message, IBus::TPtr replyBus);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
