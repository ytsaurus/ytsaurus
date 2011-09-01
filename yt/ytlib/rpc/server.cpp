#include "server.h"

#include "../misc/serialize.h"
#include "../misc/assert.h"
#include "../logging/log.h"

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

TServer::TServer(int port)
    : BusServer(New<TBusServer>(port, this))
    , Started(false)
{ }

void TServer::RegisterService(IService::TPtr service)
{
    YVERIFY(Services.insert(MakePair(service->GetServiceName(), service)).Second());
    LOG_INFO("Registered RPC service %s", ~service->GetServiceName());
}

void TServer::Start()
{
    YASSERT(!Started);
    Started = true;
    LOG_INFO("RPC server started");
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

    TRequestId requestId = TGuid::FromProto(requestHeader.GetRequestId());
    Stroka serviceName = requestHeader.GetServiceName();
    Stroka methodName = requestHeader.GetMethodName();

    LOG_DEBUG("Request received (ServiceName: %s, MethodName: %s, RequestId: %s)",
        ~serviceName,
        ~methodName,
        ~requestId.ToString());

    if (!Started) {
        IMessage::TPtr errorMessage = ~New<TRpcErrorResponseMessage>(
            requestId,
            EErrorCode::Unavailable);
        replyBus->Send(errorMessage);

        LOG_DEBUG("Server is not started");
        return;
    }

    IService::TPtr service = GetService(serviceName);
    if (~service == NULL) {
        IMessage::TPtr errorMessage = ~New<TRpcErrorResponseMessage>(
            requestId,
            EErrorCode::NoService);
        replyBus->Send(errorMessage);

        LOG_WARNING("Unknown service (ServiceName: %s)", ~serviceName);
        return;
    }

    TServiceContext::TPtr context = New<TServiceContext>(
        service,
        requestId,
        methodName,
        message,
        replyBus);
    service->OnBeginRequest(context);
}

IService::TPtr TServer::GetService(Stroka serviceName)
{
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
