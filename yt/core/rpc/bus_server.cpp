#include "stdafx.h"
#include "bus_server.h"
#include "server_detail.h"
#include "private.h"

#include <core/misc/protobuf_helpers.h>

#include <core/bus/server.h>
#include <core/bus/bus.h>

#include <core/rpc/message.h>
#include <core/rpc/rpc.pb.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;
using namespace NBus;

using ::ToString;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TBusServer
    : public TServerBase
    , public IMessageHandler
{
public:
    explicit TBusServer(IBusServerPtr busServer)
        : BusServer_(busServer)
    { }

    virtual void DoStart() override
    {
        TServerBase::DoStart();

        BusServer_->Start(this);
    }

    virtual void DoStop() override
    {
        TServerBase::DoStop();

        BusServer_->Stop();
        BusServer_.Reset();
    }

private:
    IBusServerPtr BusServer_;


    virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) override
    {
        auto messageType = GetMessageType(message);
        switch (messageType) {
            case EMessageType::Request:
                OnRequestMessage(std::move(message), std::move(replyBus));
                break;

            case EMessageType::RequestCancelation:
                OnRequestCancelationMessage(std::move(message), std::move(replyBus));
                break;

            default:
                // Unable to reply, no request id is known.
                // Let's just drop the message.
                LOG_ERROR("Invalid incoming message type %x, ignored", static_cast<ui32>(messageType));
                break;
        }
    }

    void OnRequestMessage(TSharedRefArray message, IBusPtr replyBus)
    {
        auto header = std::make_unique<NProto::TRequestHeader>();
        if (!ParseRequestHeader(message, header.get())) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            LOG_ERROR("Error parsing request header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header->request_id());
        const auto& serviceName = header->service();
        const auto& methodName = header->method();
        auto realmId = header->has_realm_id() ? FromProto<TRealmId>(header->realm_id()) : NullRealmId;
        bool oneWay = header->has_one_way() ? header->one_way() : false;
        auto timeout = header->has_timeout() ? MakeNullable(TDuration(header->timeout())) : Null;
        auto requestStartTime = header->has_request_start_time() ? MakeNullable(header->request_start_time()) : Null;
        auto retryStartTime = header->has_retry_start_time() ? MakeNullable(header->retry_start_time()) : Null;

        if (message.Size() < 2) {
            LOG_ERROR("Too few request parts: expected >= 2, actual %v (RequestId: %v)",
                message.Size(),
                requestId);
            return;
        }

        LOG_DEBUG("Request received (Method: %v:%v, RealmId: %v, RequestId: %v, OneWay: %v, Timeout: %v, RequestStartTime: %v, RetryStartTime: %v)",
            serviceName,
            methodName,
            realmId,
            requestId,
            oneWay,
            timeout,
            requestStartTime,
            retryStartTime);

        if (!Started_) {
            auto error = TError(NRpc::EErrorCode::Unavailable, "Server is not started");

            LOG_DEBUG(error);

            if (!oneWay) {
                auto response = CreateErrorResponseMessage(requestId, error);
                replyBus->Send(response, EDeliveryTrackingLevel::None);
            }
            return;
        }

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            auto error = TError(
                EErrorCode::NoSuchService,
                "Service is not registered (Service: %v, RealmId: %v, RequestId: %v)",
                serviceName,
                realmId,
                requestId);

            LOG_WARNING(error);

            if (!oneWay) {
                auto response = CreateErrorResponseMessage(requestId, error);
                replyBus->Send(response, EDeliveryTrackingLevel::None);
            }
            return;
        }

        service->HandleRequest(
            std::move(header),
            std::move(message),
            std::move(replyBus));
    }

    void OnRequestCancelationMessage(TSharedRefArray message, IBusPtr /*replyBus*/)
    {
        NProto::TRequestCancelationHeader header;
        if (!ParseRequestCancelationHeader(message, &header)) {
            // Unable to reply, no request id is known.
            // Let's just drop the message.
            LOG_ERROR("Error parsing request cancelation header");
            return;
        }

        auto requestId = FromProto<TRequestId>(header.request_id());
        const auto& serviceName = header.service();
        const auto& methodName = header.method();
        auto realmId = header.has_realm_id() ? FromProto<TRealmId>(header.realm_id()) : NullRealmId;

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            LOG_DEBUG("Service is not registered (Service: %v, RealmId: %v, RequestId: %v)",
                serviceName,
                realmId,
                requestId);
            return;
        }

        LOG_DEBUG("Request cancelation received (Method: %v:%v, RealmId: %v, RequestId: %v)",
            methodName,
            serviceName,
            realmId,
            requestId);

        service->HandleRequestCancelation(requestId);
    }
};

IServerPtr CreateBusServer(NBus::IBusServerPtr busServer)
{
    return New<TBusServer>(busServer);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
