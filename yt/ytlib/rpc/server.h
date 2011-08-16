#pragma once

#include "common.h"
#include "service.h"
#include "../bus/bus_server.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public NBus::IMessageHandler
{
public:
    typedef TIntrusivePtr<TServer> TPtr;

    TServer(int port);
    ~TServer();

    void RegisterService(IService::TPtr service);
    void Start();
    void Stop();

    Stroka GetDebugInfo();

private:
    typedef yhash_map<Stroka, IService::TPtr> TServiceMap;

    NBus::TBusServer::TPtr BusServer;
    TServiceMap Services;
    volatile bool Started;

    IService::TPtr GetService(Stroka serviceName);
    virtual void OnMessage(
        NBus::IMessage::TPtr message,
        NBus::IBus::TPtr replyBus);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
