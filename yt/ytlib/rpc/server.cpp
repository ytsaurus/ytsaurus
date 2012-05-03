#include "stdafx.h"
#include "server.h"
#include "service.h"
#include <ytlib/rpc/rpc.pb.h>
#include "server_detail.h"

#include <ytlib/logging/log.h>
#include <ytlib/bus/server.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/message.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("Rpc");

////////////////////////////////////////////////////////////////////////////////

class TServiceContext
    : public TServiceContextBase
{
public:
    typedef TIntrusivePtr<TServiceContext> TPtr;

    TServiceContext(
        const NProto::TRequestHeader& header,
        IMessage::TPtr requestMessage,
        IBus::TPtr replyBus,
        IService::TPtr service,
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
    IBus::TPtr ReplyBus;
    IService::TPtr Service;
    NLog::TLogger Logger;

    virtual void DoReply(const TError& error, IMessage::TPtr responseMessage)
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

};

////////////////////////////////////////////////////////////////////////////////

class TRpcServer
    : public IServer
    , public IMessageHandler
{
public:
    typedef TIntrusivePtr<TRpcServer> TPtr;

    TRpcServer(IBusServer::TPtr busServer)
        : BusServer(busServer)
        , Started(false)
    { }

    virtual void RegisterService(IService::TPtr service)
    {
        YASSERT(service);

        YVERIFY(Services.insert(MakePair(service->GetServiceName(), service)).second);
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
    IBusServer::TPtr BusServer;
    volatile bool Started;

    yhash_map<Stroka, IService::TPtr> Services;

    IService::TPtr GetService(const Stroka& serviceName)
    {
        auto it = Services.find(serviceName);
        return it == Services.end() ? NULL : it->second;
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

        auto header = GetRequestHeader(~message);
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
            ~message,
            ~replyBus,
            ~service,
            service->GetLoggingCategory());

        service->OnBeginRequest(~context);
    }

};

IServerPtr CreateRpcServer(NBus::IBusServer::TPtr busServer)
{
    return New<TRpcServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
