#include "stdafx.h"
#include "server.h"
#include "private.h"
#include "service.h"
#include "config.h"

#include <core/concurrency/rw_spinlock.h>

#include <core/bus/server.h>
#include <core/bus/bus.h>

#include <core/ytree/fluent.h>

#include <core/rpc/message.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;
using namespace NBus;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TRpcServer
    : public IRpcServer
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

        auto serviceId = service->GetServiceId();

        {
            TWriterGuard guard(ServicesSpinLock);
            YCHECK(Services.insert(std::make_pair(serviceId, service)).second);
        }

        LOG_INFO("RPC service registered (ServiceName: %s, RealmId: %s)",
            ~serviceId.ServiceName,
            ~ToString(serviceId.RealmId));
    }

    virtual void UnregisterService(IServicePtr service) override
    {
        YCHECK(service);

        auto serviceId = service->GetServiceId();

        {
            TWriterGuard guard(ServicesSpinLock);
            YCHECK(Services.erase(serviceId) == 1);
        }

        LOG_INFO("RPC service unregistered (ServiceName: %s, RealmId: %s)",
            ~serviceId.ServiceName,
            ~ToString(serviceId.RealmId));
    }

    virtual void Configure(TServerConfigPtr config) override
    {
        for (const auto& pair : config->Services) {
            const auto& serviceName = pair.first;
            const auto& serviceConfig = pair.second;
            auto services = FindServices(serviceName);
            if (services.empty()) {
                THROW_ERROR_EXCEPTION("Cannot find RPC service %s to configure",
                    ~serviceName.Quote());
            }
            for (auto service : services) {
                service->Configure(serviceConfig);
            }
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

    TReaderWriterSpinlock ServicesSpinLock;
    yhash_map<TServiceId, IServicePtr> Services;

    IServicePtr FindService(const TServiceId& serviceId)
    {
        TReaderGuard guard(ServicesSpinLock);
        auto it = Services.find(serviceId);
        return it == Services.end() ? nullptr : it->second;
    }

    std::vector<IServicePtr> FindServices(const Stroka& serviceName)
    {
        std::vector<IServicePtr> result;
        TReaderGuard guard(ServicesSpinLock);
        for (const auto& pair : Services) {
            if (pair.first.ServiceName == serviceName) {
                result.push_back(pair.second);
            }
        }
        return result;
    }

    virtual void OnMessage(TSharedRefArray message, IBusPtr replyBus) override
    {
        if (message.Size() < 2) {
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
        const auto& serviceName = header.service();
        const auto& verb = header.verb();
        auto realmId = header.has_realm_id() ? FromProto<TRealmId>(header.realm_id()) : NullRealmId;
        bool oneWay = header.has_one_way() ? header.one_way() : false;

        LOG_DEBUG("Request received (Service: %s, Verb: %s, RealmId: %s, RequestId: %s, OneWay: %s, RequestStartTime: %s, RetryStartTime: %s)",
            ~serviceName,
            ~verb,
            ~ToString(realmId),
            ~ToString(requestId),
            ~ToString(oneWay),
            header.has_request_start_time() ? ~ToString(TInstant(header.request_start_time())) : "<Null>",
            header.has_retry_start_time() ? ~ToString(TInstant(header.retry_start_time())) : "<Null>");

        if (!Started) {
            auto error = TError(EErrorCode::Unavailable, "Server is not started");

            LOG_DEBUG(error);

            if (!oneWay) {
                replyBus->Send(CreateErrorResponseMessage(requestId, error));
            }
            return;
        }

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Service is not registered (Service: %s, RealmId: %s, RequestId: %s)",
                ~serviceName,
                ~ToString(realmId),
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

IRpcServerPtr CreateRpcServer(NBus::IBusServerPtr busServer)
{
    return New<TRpcServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
