#pragma once

#include "common.h"
#include "service.h"
#include "bus_server.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IMessageHandler
{
public:
    TServer(int port, IInvoker::TPtr invoker);

    void RegisterService(IService::TPtr service);
    void UnregisterService(IService::TPtr service);

    Stroka GetDebugInfo();

private:
    typedef yhash_map<Stroka, IService::TPtr> TServiceMap;

    IInvoker::TPtr Invoker;
    TBusServer::TPtr BusServer;
    TServiceMap Services;
    TSpinLock SpinLock; // protects Services

    IService::TPtr GetService(Stroka serviceName);
    virtual void OnMessage(IMessage::TPtr message, IBus::TPtr replyBus);
    static void OnRequest(IService::TPtr service, TServiceContext::TPtr context);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
