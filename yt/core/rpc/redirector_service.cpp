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

static const auto& Logger = RpcServerLogger;

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

typedef TCallback<void(TSharedRefArray)> TResponseMessageHandler;

class TRedirectedResponseHandler
    : public IClientResponseHandler
{
public:
    TRedirectedResponseHandler(
        IClientRequestPtr request,
        TResponseMessageHandler responseMessageHandler)
        : Request(request)
        , ResponseMessageHandler(responseMessageHandler)
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

        ResponseMessageHandler.Run(std::move(message));
    }

    virtual void OnError(const TError& error) override
    {
        LOG_DEBUG(error, "Redirected request failed (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        auto message = CreateErrorResponseMessage(Request->GetRequestId(), error);
        ResponseMessageHandler.Run(std::move(message));
    }

private:
    IClientRequestPtr Request;
    TResponseMessageHandler ResponseMessageHandler;

};

void DoRedirectServiceRequest(
    std::unique_ptr<NProto::TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    TResponseMessageHandler responseMessageHandler,
    IChannelPtr channel)
{
    auto request = New<TRedirectedRequest>(
        std::move(requestHeader),
        std::move(requestMessage));

    LOG_DEBUG("Redirected request sent (RequestId: %s, Method: %s:%s)",
            ~ToString(request->GetRequestId()),
            ~request->GetService(),
            ~request->GetMethod());

    auto responseHandler = New<TRedirectedResponseHandler>(
        request,
        responseMessageHandler);

    channel->Send(
        request,
        responseHandler,
        Null,
        true);
}

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
        auto responseMessageHandler = BIND([=] (TSharedRefArray message) {
            replyBus->Send(std::move(message), EDeliveryTrackingLevel::None);
        });

        DoRedirectServiceRequest(
            std::move(header),
            std::move(message),
            std::move(responseMessageHandler),
            SinkChannel);
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

void RedirectServiceRequest(
    IServiceContextPtr context,
    IChannelPtr channel)
{
    auto requestHeader = std::make_unique<NProto::TRequestHeader>(context->RequestHeader());

    auto responseMessageHandler = BIND([=] (TSharedRefArray message) {
        context->Reply(std::move(message));
    });

    DoRedirectServiceRequest(
        std::move(requestHeader),
        context->GetRequestMessage(),
        std::move(responseMessageHandler),
        std::move(channel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
