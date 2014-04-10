#include "stdafx.h"
#include "redirector_service.h"
#include "client.h"
#include "service.h"
#include "message.h"
#include "private.h"

#include <core/bus/bus.h>

#include <core/ytree/node.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

namespace {

class TRedirectedRequest
    : public IClientRequest
{
public:
    TRedirectedRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message)
        : Header_(std::move(header))
        , Message(std::move(message))
        , RequestId(FromProto<TRequestId>(Header_->request_id()))
    { }

    virtual TSharedRefArray Serialize() const override
    {
        return Message;
    }

    virtual bool IsOneWay() const override
    {
        return Header_->one_way();
    }

    virtual bool IsRequestHeavy() const override
    {
        return false;
    }

    virtual bool IsResponseHeavy() const override
    {
        return false;
    }

    virtual TRequestId GetRequestId() const override
    {
        return RequestId;
    }

    virtual const Stroka& GetService() const override
    {
        return Header_->service();
    }

    virtual const Stroka& GetMethod() const override
    {
        return Header_->method();
    }

    virtual TInstant GetStartTime() const override
    {
        YUNREACHABLE();
    }

    virtual void SetStartTime(TInstant /*value*/) override
    {
        YUNREACHABLE();
    }

    virtual const NProto::TRequestHeader& Header() const override
    {
        return *Header_;
    }

    virtual NProto::TRequestHeader& Header() override
    {
        return *Header_;
    }

private:
    std::unique_ptr<NProto::TRequestHeader> Header_;
    TSharedRefArray Message;

    TRequestId RequestId;

};

////////////////////////////////////////////////////////////////////////////////

class TRedirectedResponseHandler
    : public IClientResponseHandler
{
public:
    TRedirectedResponseHandler(IClientRequestPtr request, IBusPtr replyBus)
        : Request(request)
        , ReplyBus(replyBus)
    { }

    virtual void OnAcknowledgement() override
    {
        LOG_DEBUG("Redirected request acknowledged (RequestId: %s)",
            ~ToString(Request->GetRequestId()));
    }

    virtual void OnResponse(TSharedRefArray message) override
    {
        LOG_DEBUG("Response for redirected request received (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        ReplyBus->Send(message, EDeliveryTrackingLevel::None);
    }

    virtual void OnError(const TError& error) override
    {
        LOG_DEBUG(error, "Redirected request failed (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        auto message = CreateErrorResponseMessage(Request->GetRequestId(), error);
        ReplyBus->Send(message, EDeliveryTrackingLevel::None);
    }

private:
    IClientRequestPtr Request;
    IBusPtr ReplyBus;

};

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TRedirectorService
    : public IService
{
public:
    TRedirectorService(
        const TServiceId& serviceId,
        IChannelPtr sinkChannel)
        : ServiceId(serviceId)
        , SinkChannel(sinkChannel)
    { }

    virtual void OnRequest(
        std::unique_ptr<NProto::TRequestHeader> header,
        TSharedRefArray message,
        IBusPtr replyBus) override
    {
        auto request = New<TRedirectedRequest>(
            std::move(header),
            std::move(message));

        LOG_DEBUG("Redirecting request (RequestId: %s, Service: %s, Method: %s)",
            ~ToString(request->GetRequestId()),
            ~request->GetService(),
            ~request->GetMethod());

        auto responseHandler = New<TRedirectedResponseHandler>(request, replyBus);
        SinkChannel->Send(
            request,
            responseHandler,
            Null,
            true);
    }

    virtual TServiceId GetServiceId() const override
    {
        return ServiceId;
    }

    virtual void Configure(NYTree::INodePtr config) override
    {
        UNUSED(config);
    }

private:
    TServiceId ServiceId;
    Stroka LoggingCategory;
    IChannelPtr SinkChannel;

};

IServicePtr CreateRedirectorService(
    const TServiceId& serviceId,
    IChannelPtr sinkChannel)
{
    YCHECK(sinkChannel);

    return New<TRedirectorService>(serviceId, sinkChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
