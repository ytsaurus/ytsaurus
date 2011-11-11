#include "stdafx.h"
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

TServer::~TServer()
{ }

void TServer::RegisterService(IService* service)
{
    YASSERT(service != NULL);

    YVERIFY(Services.insert(MakePair(service->GetServiceName(), service)).Second());
    LOG_INFO("RPC service registered (ServiceName: %s)", ~service->GetServiceName());
}

void TServer::Start()
{
    YASSERT(!Started);
    Started = true;
    LOG_INFO("RPC server started");
}

void TServer::Stop()
{
    if (!Started)
        return;
    Started = false;
    BusServer->Terminate();
    LOG_INFO("RPC server stopped");
}

void TServer::OnMessage(IMessage::TPtr message, IBus::TPtr replyBus)
{
    const auto& parts = message->GetParts();
    if (parts.ysize() < 2) {
        LOG_WARNING("Too few message parts");
        return;
    }

    TRequestHeader requestHeader;
    if (!DeserializeMessage(&requestHeader, parts[0])) {
        LOG_ERROR("Error deserializing request header");
        return;
    }

    auto requestId = TRequestId::FromProto(requestHeader.GetRequestId());
    Stroka path = requestHeader.GetPath();
    Stroka verb = requestHeader.GetVerb();

    LOG_DEBUG("Request received (Path: %s, Verb: %s, RequestId: %s)",
        ~path,
        ~verb,
        ~requestId.ToString());

    if (!Started) {
        auto errorMessage = New<TRpcErrorResponseMessage>(
            requestId,
            TError(EErrorCode::Unavailable));
        replyBus->Send(errorMessage);

        LOG_DEBUG("Server is not started");
        return;
    }

    // TODO: anything smarter?
    Stroka serviceName = path;

    auto service = GetService(serviceName);
    if (~service == NULL) {
        auto errorMessage = New<TRpcErrorResponseMessage>(
            requestId,
            TError(EErrorCode::NoSuchService));
        replyBus->Send(errorMessage);

        LOG_WARNING("Unknown service (ServiceName: %s)", ~serviceName);
        return;
    }

    auto context = New<TServiceContext>(
        ~service,
        requestId,
        path,
        verb,
        ~message);

    service->OnBeginRequest(~context, ~replyBus);
}

IService::TPtr TServer::GetService(const Stroka& serviceName)
{
    auto it = Services.find(serviceName);
    return it == Services.end() ? NULL : it->Second();
}

Stroka TServer::GetDebugInfo()
{
    return BusServer->GetDebugInfo();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
