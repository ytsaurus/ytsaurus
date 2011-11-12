#include "stdafx.h"
#include "server.h"
#include "rpc.pb.h"
#include "server_detail.h"

#include "../misc/assert.h"
#include "../logging/log.h"
#include "../bus/bus_server.h"

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NProto;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    typedef TIntrusivePtr<TServiceContext> TPtr;

    TServiceContext(
        const TRequestId& requestId,
        const Stroka& path,
        const Stroka& verb,
        IMessage* requestMessage,
        IBus* replyBus,
        IService* service,
        const Stroka& loggingCategory)
        : TServiceContextBase(requestId, path, verb, requestMessage)
        , RequestId(requestId)
        , ReplyBus(replyBus)
        , Service(service)
        , Logger(loggingCategory)
    {
        YASSERT(replyBus != NULL);
        YASSERT(service != NULL);
        YASSERT(service != NULL);
    }

private:
    TRequestId RequestId;
    IBus::TPtr ReplyBus;
    IService::TPtr Service;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage* responseMessage)
    {
        UNUSED(error);

        ReplyBus->Send(responseMessage);
        Service->OnEndRequest(this);
    }

    virtual void LogRequest()
    {
        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, RequestInfo);
        LOG_DEBUG("%s <- %s",
            ~Verb,
            ~str);
    }

    virtual void LogResponse(const TError& error)
    {
        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, Sprintf("Error: %s", ~error.ToString()));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s -> %s",
            ~Verb,
            ~str);
    }

    virtual void LogException(const Stroka& message)
    {
        Stroka str;
        AppendInfo(str, Sprintf("RequestId: %s", ~RequestId.ToString()));
        AppendInfo(str, Sprintf("Path: %s", ~Path));
        AppendInfo(str, Sprintf("Verb: %s", ~Verb));
        AppendInfo(str, ResponseInfo);
        LOG_FATAL("Unhandled exception in RPC service method (%s): %s",
            ~str,
            ~message);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TServer
    : public IServer
    , public IMessageHandler
{
public:
    typedef TIntrusivePtr<TServer> TPtr;

    TServer(int port)
        : BusServer(New<TBusServer>(port, this))
        , Started(false)
    { }

    virtual void RegisterService(IService* service)
    {
        YASSERT(service != NULL);

        YVERIFY(Services.insert(MakePair(service->GetServiceName(), service)).Second());
        LOG_INFO("RPC service registered (ServiceName: %s)", ~service->GetServiceName());
    }

    virtual void Start()
    {
        YASSERT(!Started);
        Started = true;
        LOG_INFO("RPC server started");
    }

    virtual void Stop()
    {
        if (!Started)
            return;
        Started = false;
        BusServer->Terminate();
        LOG_INFO("RPC server stopped");
    }

    virtual Stroka GetDebugInfo()
    {
        return BusServer->GetDebugInfo();
    }

private:
    TBusServer::TPtr BusServer;
    yhash_map<Stroka, IService::TPtr> Services;
    volatile bool Started;

    IService::TPtr GetService(const Stroka& serviceName)
    {
        auto it = Services.find(serviceName);
        return it == Services.end() ? NULL : it->Second();
    }

    virtual void OnMessage(
        IMessage::TPtr message,
        IBus::TPtr replyBus)
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
            auto errorMessage = CreateErrorResponseMessage(
                requestId,
                TError(EErrorCode::Unavailable));
            replyBus->Send(errorMessage);

            LOG_DEBUG("Server is not started (RequestId: %s)",
                ~requestId.ToString());
            return;
        }

        // TODO: anything smarter?
        Stroka serviceName = path;

        auto service = GetService(serviceName);
        if (~service == NULL) {
            auto errorMessage = CreateErrorResponseMessage(
                requestId,
                TError(EErrorCode::NoSuchService));
            replyBus->Send(errorMessage);

            LOG_DEBUG("Unknown service name (RequestId: %s, ServiceName: %s)",
                ~requestId.ToString(),
                ~serviceName);
            return;
        }

        auto context = New<TServiceContext>(
            requestId,
            path,
            verb,
            ~message,
            ~replyBus,
            ~service,
            service->GetLoggingCategory());

        service->OnBeginRequest(~context);
    }

};

IServer::TPtr CreateRpcServer(int port)
{
    return New<TServer>(port);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
