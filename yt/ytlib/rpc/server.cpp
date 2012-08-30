#include "stdafx.h"
#include "server.h"
#include "private.h"
#include "service.h"
#include "server_detail.h"

#include <ytlib/bus/server.h>
#include <ytlib/bus/bus.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/message.h>
#include <ytlib/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    TServiceContext(
        const NProto::TRequestHeader& header,
        IMessagePtr requestMessage,
        IBusPtr replyBus,
        IServicePtr service,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ReplyBus(replyBus)
        , Service(service)
        , Logger(loggingCategory)
    {
        YASSERT(replyBus);
        YASSERT(service);
    }

private:
    IBusPtr ReplyBus;
    IServicePtr Service;
    NLog::TLogger Logger;

    virtual void DoReply(IMessagePtr responseMessage)
    {
        ReplyBus->Send(responseMessage);
        Service->OnEndRequest(MakeStrong(this));
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
        AppendInfo(str, Sprintf("Error: %s", ~ToString(error)));
        AppendInfo(str, ResponseInfo);
        LOG_DEBUG("%s -> %s",
            ~Verb,
            ~str);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TRpcServer
    : public IServer
    , public IMessageHandler
{
public:
    TRpcServer(IBusServerPtr busServer)
        : BusServer(busServer)
        , Started(false)
    { }

    virtual void RegisterService(IServicePtr service)
    {
        YASSERT(service);

        YCHECK(Services.insert(MakePair(service->GetServiceName(), service)).second);
        LOG_INFO("RPC service registered (ServiceName: %s)", ~service->GetServiceName());
    }

    virtual void Start()
    {
        YASSERT(!Started);

        Started = true;
        BusServer->Start(this);

        LOG_INFO("RPC server started");
    }

    virtual void Stop()
    {
        if (!Started)
            return;

        Started = false;
        BusServer->Stop();
        BusServer.Reset();

        LOG_INFO("RPC server stopped");
    }

private:
    IBusServerPtr BusServer;
    volatile bool Started;

    yhash_map<Stroka, IServicePtr> Services;

    IServicePtr GetService(const Stroka& serviceName)
    {
        auto it = Services.find(serviceName);
        return it == Services.end() ? NULL : it->second;
    }

    virtual void OnMessage(
        IMessagePtr message,
        IBusPtr replyBus)
    {
        const auto& parts = message->GetParts();
        if (parts.size() < 2) {
            LOG_WARNING("Too few message parts");
            return;
        }

        NProto::TRequestHeader header;
        if (!ParseRequestHeader(message, &header)) {
            // Unable to reply, no requestId is known.
            // Let's just drop the message.
            LOG_ERROR("Error parsing request header");
            return;
        }

        auto requestId = TRequestId::FromProto(header.request_id());
        Stroka path = header.path();
        Stroka verb = header.verb();
        bool oneWay = header.has_one_way() ? header.one_way() : false;

        LOG_DEBUG("Request received (Path: %s, Verb: %s, RequestId: %s, OneWay: %s)",
            ~path,
            ~verb,
            ~requestId.ToString(),
            ~ToString(oneWay));

        if (!Started) {
            Stroka message = Sprintf("Server is not started (RequestId: %s)",
                ~requestId.ToString());
            LOG_DEBUG("%s", ~message);

            auto response = CreateErrorResponseMessage(
                requestId,
                TError(EErrorCode::Unavailable, message));
            replyBus->Send(response);
            return;
        }

        // TODO: anything smarter?
        Stroka serviceName = path;

        auto service = GetService(serviceName);
        if (!service) {
            Stroka message = Sprintf("Unknown service name %s (RequestId: %s)",
                ~serviceName.Quote(),
                ~requestId.ToString());

            if (!oneWay) {
                auto response = CreateErrorResponseMessage(
                    requestId,
                    TError(EErrorCode::NoSuchService, message));
                replyBus->Send(response);
            }

            LOG_DEBUG("%s", ~message);
            return;
        }

        auto context = New<TServiceContext>(
            header,
            message,
            replyBus,
            service,
            service->GetLoggingCategory());

        service->OnBeginRequest(context);
    }

};

IServerPtr CreateRpcServer(NBus::IBusServerPtr busServer)
{
    return New<TRpcServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
