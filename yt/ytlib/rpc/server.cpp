#include "server.h"

#include "../misc/serialize.h"
#include "../misc/string.h"
#include "../logging/log.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(int port, IInvoker::TPtr invoker)
    : Invoker(invoker)
    , BusServer(new TBusServer(port, this))
{ }

void TServer::RegisterService(IService::TPtr service)
{
    Stroka name = service->GetServiceName();
    TGuard<TSpinLock> guard(SpinLock);
    bool inserted = Services.insert(MakePair(name, service)).Second();
    if (!inserted) {
        throw yexception() << "Service " << name << " is already registered";
    }
}

void TServer::UnregisterService(IService::TPtr service)
{
    Stroka name = service->GetServiceName();
    TGuard<TSpinLock> guard(SpinLock);
    TServiceMap::size_type erased = Services.erase(name);
    if (erased == 0) {
        throw yexception() << "Service " << name << " is not registered";
    }
}

void TServer::OnMessage(IMessage::TPtr message, IBus::TPtr replyBus)
{
    const yvector<TSharedRef>& parts = message->GetParts();
    if (parts.ysize() < 2) {
        LOG_WARNING("Too few message parts");
        return;
    }

    TRequestHeader requestHeader;
    if (!DeserializeMessage(&requestHeader, parts[0])) {
        LOG_ERROR("Error deserializing request header");
        return;
    }

    TRequestId requestId = GuidFromProtoGuid(requestHeader.GetRequestId());
    Stroka serviceName = requestHeader.GetServiceName();
    Stroka methodName = requestHeader.GetMethodName();

    LOG_DEBUG("Request received (RequestId: %s, ServiceName: %s, MethodName: %s)",
        ~StringFromGuid(requestId), ~serviceName, ~methodName);

    IService::TPtr service = GetService(serviceName);
    if (~service == NULL) {
        LOG_WARNING("Unknown service (ServiceName: %s)",
            ~serviceName);

        IMessage::TPtr errorMessage = new TRpcErrorResponseMessage(
            requestId,
            EErrorCode::NoService);
        replyBus->Send(errorMessage);
        return;
    }

    TServiceContext::TPtr context = new TServiceContext(
        service,
        requestId,
        methodName,
        message,
        replyBus);

    Invoker->Invoke(FromMethod(
        &TServer::OnRequest,
        service,
        context));
}

void TServer::OnRequest(IService::TPtr service, TServiceContext::TPtr context)
{
    service->OnRequest(context);
}

IService::TPtr TServer::GetService(Stroka serviceName)
{
    TGuard<TSpinLock> guard(SpinLock);
    TServiceMap::iterator it = Services.find(serviceName);
    if (it == Services.end()) {
        return NULL;
    }
    return it->Second();
}

Stroka TServer::GetDebugInfo()
{
    return BusServer->GetDebugInfo();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
