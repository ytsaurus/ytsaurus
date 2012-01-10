#include "stdafx.h"
#include "server.h"
#include "rpc.pb.h"
#include "server_detail.h"

#include <ytlib/misc/assert.h>
#include <ytlib/logging/log.h>
#include <ytlib/bus/server.h>
#include <ytlib/ytree/fluent.h>
#include <ytlib/rpc/message.h>

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
        const TRequestHeader& header,
        IMessage* requestMessage,
        IBus* replyBus,
        IService* service,
        const Stroka& loggingCategory)
        : TServiceContextBase(header, requestMessage)
        , ReplyBus(replyBus)
        , Service(service)
        , Logger(loggingCategory)
    {
        YASSERT(replyBus);
        YASSERT(service);
        YASSERT(service);
    }

private:
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
        LOG_FATAL("Unhandled exception in RPC service method (%s)\n%s",
            ~str,
            ~message);
    }

};

////////////////////////////////////////////////////////////////////////////////

class TRpcServer
    : public IRpcServer
    , public IMessageHandler
{
public:
    typedef TIntrusivePtr<TRpcServer> TPtr;

    TRpcServer(IBusServer* busServer)
        : BusServer(busServer)
        , Started(false)
    {
    }

    virtual void RegisterService(IService* service)
    {
        YASSERT(service);

        YVERIFY(Services.insert(MakePair(service->GetServiceName(), service)).Second());
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

    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer)
    {
        // TODO: more
        BuildYsonFluently(consumer)
            .BeginMap()
            .EndMap();
    }

private:
    IBusServer::TPtr BusServer;
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
            Stroka message = Sprintf("Unknown service name (RequestId: %s, ServiceName: %s)",
                ~requestId.ToString(),
                ~serviceName);

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

IRpcServer::TPtr CreateRpcServer(NBus::IBusServer* busServer)
{
    return New<TRpcServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
