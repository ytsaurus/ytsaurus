#include "redirector_service.h"
#include "private.h"
#include "channel_detail.h"
#include "client.h"
#include "message.h"
#include "service.h"

#include <yt/core/bus/bus.h>

#include <yt/core/ytree/node.h>

namespace NYT::NRpc {

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
        if (!FirstTimeSerialization_) {
            Header_->set_retry(true);
        }

        FirstTimeSerialization_ = false;

        Y_ASSERT(Message_.Size() >= 2);

        auto body = Message_[1];
        auto attachments = std::vector<TSharedRef>(Message_.Begin() + 2, Message_.End());

        return CreateRequestMessage(*Header_, body, attachments);
    }

    virtual const TRequestHeader& Header() const override
    {
        return *Header_;
    }

    virtual TRequestHeader& Header() override
    {
        return *Header_;
    }

    virtual bool IsHeavy() const override
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

    virtual const TString& GetService() const override
    {
        return Header_->service();
    }

    virtual const TString& GetMethod() const override
    {
        return Header_->method();
    }

    virtual const TString& GetUser() const override
    {
        return Header_->has_user()
            ? Header_->user()
            : RootUserName;
    }

    virtual void SetUser(const TString& user) override
    {
        if (user == RootUserName) {
            Header_->clear_user();
        } else {
            Header_->set_user(user);
        }
    }

    virtual void SetUserAgent(const TString& userAgent) override
    {
        Header_->set_user_agent(userAgent);
    }

    virtual bool GetRetry() const override
    {
        Y_UNREACHABLE();
    }

    virtual void SetRetry(bool /*value*/) override
    {
        Y_UNREACHABLE();
    }

    virtual TMutationId GetMutationId() const override
    {
        Y_UNREACHABLE();
    }

    virtual void SetMutationId(const TMutationId& /*id*/) override
    {
        Y_UNREACHABLE();
    }

    virtual size_t GetHash() const override
    {
        return 0;
    }

    virtual EMultiplexingBand GetMultiplexingBand() const override
    {
        return EMultiplexingBand::Default;
    }

    virtual void SetMultiplexingBand(EMultiplexingBand /*band*/) override
    {
        Y_UNREACHABLE();
    }

private:
    const std::unique_ptr<TRequestHeader> Header_;
    const TSharedRefArray Message_;

    bool FirstTimeSerialization_ = true;

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
        ? std::make_optional(FromProto<TDuration>(requestHeader->timeout()))
        : std::nullopt;

    auto request = New<TRedirectedRequest>(
        std::move(requestHeader),
        std::move(requestMessage));

    auto responseHandler = New<TRedirectedResponseHandler>(
        request,
        std::move(responseMessageHandler));

    LOG_DEBUG("Request redirected (RequestId: %v, Method: %v:%v, RealmId: %v, Timeout: %v)",
        request->GetRequestId(),
        request->GetService(),
        request->GetMethod(),
        request->GetRealmId(),
        timeout);

    TSendOptions options;
    options.Timeout = timeout;
    return channel->Send(
        std::move(request),
        std::move(responseHandler),
        options);
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

        IClientRequestControlPtr existingRequestControl;
        {
            TGuard<TSpinLock> guard(SpinLock_);
            // NB: We're OK with duplicate request ids.
            auto pair = ActiveRequestMap_.insert(std::make_pair(requestId, requestControlThunk));
            if (!pair.second) {
                existingRequestControl = pair.first->second;
                pair.first->second = requestControlThunk;
            }
        }

        if (existingRequestControl) {
            existingRequestControl->Cancel();
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

        auto requestControl = it->second;
        ActiveRequestMap_.erase(it);

        guard.Release();

        requestControl->Cancel();
    }

    virtual const TServiceId& GetServiceId() const override
    {
        return ServiceId_;
    }

    virtual void Configure(NYTree::INodePtr /*config*/) override
    { }

    virtual TFuture<void> Stop() override
    {
        // TODO(babenko): should we be really tracking all outstanding request?
        return VoidFuture;
    }

private:
    const TServiceId ServiceId_;
    const IChannelPtr SinkChannel_;

    TSpinLock SpinLock_;
    THashMap<TRequestId, IClientRequestControlPtr> ActiveRequestMap_;


    void OnResponse(
        const TRequestId& requestId,
        const IBusPtr& replyBus,
        TSharedRefArray message)
    {
        {
            TGuard<TSpinLock> guard(SpinLock_);
            // NB: We're OK with duplicate request ids.
            ActiveRequestMap_.erase(requestId);
        }

        replyBus->Send(std::move(message), NBus::TSendOptions(EDeliveryTrackingLevel::None));
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

} // namespace NYT::NRpc
