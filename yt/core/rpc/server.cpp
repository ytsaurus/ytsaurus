#include "stdafx.h"
#include "server.h"
#include "private.h"
#include "service.h"
#include "config.h"

#include <core/bus/server.h>
#include <core/bus/bus.h>

#include <core/ytree/fluent.h>

#include <core/rpc/message.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;

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

        YCHECK(Services.insert(std::make_pair(service->GetServiceName(), service)).second);
        LOG_INFO("RPC service registered: %s", ~service->GetServiceName());
    }

    virtual void Configure(TServerConfigPtr config) override
    {
        FOREACH (const auto& pair, config->Services) {
            const auto& serviceName = pair.first;
            const auto& serviceConfig = pair.second;
            auto service = FindService(serviceName);
            if (!service) {
                THROW_ERROR_EXCEPTION("Cannot find RPC service to configure: %s",
                    ~serviceName);
            }
            service->Configure(serviceConfig);
        }
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

    IServicePtr FindService(const Stroka& serviceName)
    {
        auto it = Services.find(serviceName);
        return it == Services.end() ? nullptr : it->second;
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

        auto requestId = FromProto<TRequestId>(header.request_id());
        const auto& path = header.path();
        const auto& verb = header.verb();
        bool oneWay = header.has_one_way() ? header.one_way() : false;

        LOG_DEBUG("Request received (Path: %s, Verb: %s, RequestId: %s, OneWay: %s, RequestStartTime: %s, RetryStartTime: %s)",
            ~path,
            ~verb,
            ~ToString(requestId),
            ~ToString(oneWay),
            header.has_request_start_time() ? ~ToString(TInstant(header.request_start_time())) : "<Null>",
            header.has_retry_start_time() ? ~ToString(TInstant(header.retry_start_time())) : "<Null>");

        if (!Started) {
            auto error = TError(
                EErrorCode::Unavailable,
                "Server is not started (RequestId: %s)",
                ~ToString(requestId));

            LOG_DEBUG(error);

            if (!oneWay) {
                replyBus->Send(CreateErrorResponseMessage(requestId, error));
            }
            return;
        }

        // TODO: anything smarter?
        const auto& serviceName = path;

        auto service = FindService(serviceName);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Unknown service %s (RequestId: %s)",
                ~serviceName.Quote(),
                ~ToString(requestId));

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
