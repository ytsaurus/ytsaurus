#include "stdafx.h"
#include "redirector_service.h"
#include "client.h"
#include "service.h"
#include "message.h"
#include "channel_detail.h"
#include "private.h"

#include <core/bus/bus.h>

#include <core/ytree/node.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NRpc::NProto;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcServerLogger;

////////////////////////////////////////////////////////////////////////////////

class TRedirectedRequest
    : public IClientRequest
{
public:
    TRedirectedRequest(
        std::unique_ptr<TRequestHeader> header,
        TSharedRefArray message)
        : Header_(std::move(header))
        , Message_(std::move(message))
    { }

    virtual TSharedRefArray Serialize() override
    {
        return Message_;
    }

    virtual const TRequestHeader& Header() const override
    {
        return *Header_;
    }

    virtual TRequestHeader& Header() override
    {
        return *Header_;
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
        return FromProto<TRequestId>(Header_->request_id());
    }

    virtual TRealmId GetRealmId() const override
    {
        return FromProto<TRealmId>(Header_->realm_id());
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

private:
    const std::unique_ptr<TRequestHeader> Header_;
    const TSharedRefArray Message_;

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
        : Request_(request)
        , ResponseMessageHandler_(responseMessageHandler)
    { }

    virtual void HandleAcknowledgement() override
    {
        LOG_DEBUG("Redirected request acknowledged (RequestId: %v)",
            Request_->GetRequestId());
    }

    virtual void HandleResponse(TSharedRefArray message) override
    {
        LOG_DEBUG("Response for redirected request received (RequestId: %v)",
            Request_->GetRequestId());

        ResponseMessageHandler_.Run(std::move(message));
    }

    virtual void HandleError(const TError& error) override
    {
        LOG_DEBUG(error, "Redirected request failed (RequestId: %v)",
            Request_->GetRequestId());

        auto message = CreateErrorResponseMessage(Request_->GetRequestId(), error);
        ResponseMessageHandler_.Run(std::move(message));
    }

private:
    const IClientRequestPtr Request_;
    const TResponseMessageHandler ResponseMessageHandler_;

};

namespace {

IClientRequestControlPtr DoRedirectServiceRequest(
    std::unique_ptr<TRequestHeader> requestHeader,
    TSharedRefArray requestMessage,
    TResponseMessageHandler responseMessageHandler,
    IChannelPtr channel)
{
    auto timeout = requestHeader->has_timeout()
        ? MakeNullable(TDuration::MilliSeconds(requestHeader->timeout()))
        : Null;

    auto request = New<TRedirectedRequest>(
        std::move(requestHeader),
        std::move(requestMessage));

    LOG_DEBUG("Redirected request sent (RequestId: %v, Method: %v:%v, RealmId: %v, Timeout: %v)",
        request->GetRequestId(),
        request->GetService(),
        request->GetMethod(),
        request->GetRealmId(),
        timeout);

    auto responseHandler = New<TRedirectedResponseHandler>(
        request,
        std::move(responseMessageHandler));

    return channel->Send(
        std::move(request),
        std::move(responseHandler),
        timeout,
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
        : ServiceId_(serviceId)
        , SinkChannel_(sinkChannel)
    { }

    virtual void HandleRequest(
        std::unique_ptr<TRequestHeader> header,
        TSharedRefArray message,
        IBusPtr replyBus) override
    {
        auto requestId = FromProto<TRequestId>(header->request_id());
        auto requestControlThunk = New<TClientRequestControlThunk>();

        {
            TGuard<TSpinLock> guard(SpinLock_);
            // NB: We're OK with duplicate request ids.
            ActiveRequestMap_.insert(std::make_pair(requestId, requestControlThunk));
        }

        auto requestControl = DoRedirectServiceRequest(
            std::move(header),
            std::move(message),
            BIND(&TRedirectorService::OnResponse, MakeStrong(this), requestId, std::move(replyBus)),
            SinkChannel_);
        requestControlThunk->SetUnderlying(std::move(requestControl));
    }

    virtual void HandleRequestCancelation(const TRequestId& requestId) override
    {
        TGuard<TSpinLock> guard(SpinLock_);
        auto it = ActiveRequestMap_.find(requestId);
        if (it == ActiveRequestMap_.end()) {
            LOG_DEBUG("Attempt to cancel an unknown request, ignored (RequestId: %v)",
                requestId);
            return;
        }

    }

    virtual TServiceId GetServiceId() const override
    {
        return ServiceId_;
    }

    virtual void Configure(NYTree::INodePtr /*config*/) override
    { }

private:
    const TServiceId ServiceId_;
    const IChannelPtr SinkChannel_;
    
    TSpinLock SpinLock_;
    yhash_map<TRequestId, IClientRequestControlPtr> ActiveRequestMap_;
    
    
    void OnResponse(
        const TRequestId& requestId,
        IBusPtr replyBus,
        TSharedRefArray message)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            // NB: We're OK with duplicate request ids.
            ActiveRequestMap_.erase(requestId);
        }
        replyBus->Send(std::move(message), EDeliveryTrackingLevel::None);
    }

};

IServicePtr CreateRedirectorService(
    const TServiceId& serviceId,
    IChannelPtr sinkChannel)
{
    YCHECK(sinkChannel);

    return New<TRedirectorService>(serviceId, sinkChannel);
}

////////////////////////////////////////////////////////////////////////////////

IClientRequestControlPtr RedirectServiceRequest(
    IServiceContextPtr context,
    IChannelPtr channel)
{
    YCHECK(context);
    YCHECK(channel);

    auto requestHeader = std::make_unique<TRequestHeader>(context->RequestHeader());

    auto responseMessageHandler = BIND([=] (TSharedRefArray message) {
        context->Reply(std::move(message));
    });

    return DoRedirectServiceRequest(
        std::move(requestHeader),
        context->GetRequestMessage(),
        std::move(responseMessageHandler),
        std::move(channel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
