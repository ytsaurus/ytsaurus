#include "server.h"

#include <yt/core/rpc/server_detail.h>
#include <yt/core/rpc/private.h>

#include <yt/core/bus/bus.h>
#include <yt/core/bus/server.h>

#include <yt/core/misc/protobuf_helpers.h>

#include <yt/core/rpc/message.h>
#include <yt/core/rpc/proto/rpc.pb.h>

namespace NYT::NRpc::NBus {

using namespace NConcurrency;
using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

class TBusServer
    : public TServerBase
    , public IMessageHandler
{
public:
    explicit TBusServer(IBusServerPtr busServer)
        : TServerBase(NLogging::TLogger(RpcServerLogger)
             .AddTag("ServerId: %v", TGuid::Create()))
        , BusServer_(std::move(busServer))
    { }

private:
    IBusServerPtr BusServer_;


    virtual void DoStart() override
    {
        BusServer_->Start(this);
        TServerBase::DoStart();
    }

    virtual TFuture<void> DoStop(bool graceful) override
    {
        return TServerBase::DoStop(graceful).Apply(BIND([=, this_ = MakeStrong(this)] (const TError& error) {
            // NB: Stop the bus server anyway.
            auto asyncResult = BusServer_->Stop();
            BusServer_.Reset();
            error.ThrowOnError();
            return asyncResult;
        }));
    }

    virtual void HandleMessage(TSharedRefArray message, IBusPtr replyBus) noexcept override
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
                LOG_ERROR("Incoming message has invalid type, ignored (Type: %x)",
                    static_cast<ui32>(messageType));
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
        auto realmId = FromProto<TRealmId>(header->realm_id());
        auto tosLevel = header->tos_level();

        if (message.Size() < 2) {
            LOG_ERROR("Too few request parts: expected >= 2, actual %v (RequestId: %v)",
                message.Size(),
                requestId);
            return;
        }

        LOG_DEBUG("Request received (RequestId: %v)",
            requestId);

        auto replyWithError = [&] (const TError& error) {
            LOG_DEBUG(error);
            auto response = CreateErrorResponseMessage(requestId, error);
            replyBus->Send(std::move(response), TSendOptions(EDeliveryTrackingLevel::None));
        };

        if (!Started_) {
            replyWithError(TError(
                NRpc::EErrorCode::Unavailable,
                "Server is not started")
                << TErrorAttribute("realm_id", realmId));
            return;
        }

        TServiceId serviceId(serviceName, realmId);
        auto service = FindService(serviceId);
        if (!service) {
            replyWithError(TError(
                EErrorCode::NoSuchService,
                "Service is not registered")
                 << TErrorAttribute("service", serviceName)
                 << TErrorAttribute("realm_id", realmId));
            return;
        }

        replyBus->SetTosLevel(tosLevel);

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
        auto realmId = FromProto<TRealmId>(header.realm_id());

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
            serviceName,
            methodName,
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

} // namespace NYT::NRpc::NBus
