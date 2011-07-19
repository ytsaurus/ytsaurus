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

    TServer(int port, IInvoker::TPtr invoker);

    void RegisterService(IService::TPtr service);
    void UnregisterService(IService::TPtr service);

    IInvoker::TPtr GetInvoker();

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
