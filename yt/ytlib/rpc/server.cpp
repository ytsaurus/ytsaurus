#include "stdafx.h"
#include "server.h"
#include "private.h"
#include "service.h"

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

class TRpcServer
    : public IServer
    , public IMessageHandler
{
public:
    explicit TRpcServer(IBusServerPtr busServer)
        : BusServer(busServer)
        , Started(false)
    { }

    virtual void RegisterService(IServicePtr service) override
    {
        YCHECK(service);

        YCHECK(Services.insert(MakePair(service->GetServiceName(), service)).second);
        LOG_INFO("RPC service registered: %s", ~service->GetServiceName());
    }

    virtual void Start() override
    {
        YCHECK(!Started);

        Started = true;
        BusServer->Start(this);

        LOG_INFO("RPC server started");
    }

    virtual void Stop() override
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

    virtual void OnMessage(IMessagePtr message, IBusPtr replyBus) override
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
            auto error = TError(
                EErrorCode::Unavailable,
                "Server is not started (RequestId: %s)",
                ~requestId.ToString());

            LOG_DEBUG(error);

            if (!oneWay) {
                replyBus->Send(CreateErrorResponseMessage(requestId, error));
            }
            return;
        }

        // TODO: anything smarter?
        const auto& serviceName = path;

        auto service = GetService(serviceName);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Unknown service: %s (RequestId: %s)",
                ~serviceName.Quote(),
                ~requestId.ToString());

            LOG_WARNING(error);

            if (!oneWay) {
                auto response = CreateErrorResponseMessage(requestId, error);
                replyBus->Send(response);
            }
            return;
        }

        service->OnRequest(header, message, replyBus);
    }

};

IServerPtr CreateRpcServer(NBus::IBusServerPtr busServer)
{
    return New<TRpcServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
