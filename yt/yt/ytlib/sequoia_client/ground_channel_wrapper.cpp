#include "ground_channel_wrapper.h"

#include <yt/yt/core/rpc/channel.h>
#include <yt/yt/core/rpc/client.h>

namespace NYT::NSequoiaClient {

////////////////////////////////////////////////////////////////////////////////

using namespace NRpc;

///////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TClientResponseHandlerWrapper)

class TClientResponseHandlerWrapper
    : public IClientResponseHandler
{
public:
    explicit TClientResponseHandlerWrapper(IClientResponseHandlerPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    void HandleAcknowledgement() override
    {
        Underlying_->HandleAcknowledgement();
    }

    void HandleResponse(TSharedRefArray message, const std::string& address) override
    {
        Underlying_->HandleResponse(std::move(message), address);
    }

    void HandleError(TError error) override
    {
        Underlying_->HandleError(
            TError(EErrorCode::SequoiaRetriableError, "Sequoia ground request failed")
            << error);
    }

    void HandleStreamingPayload(const TStreamingPayload& payload) override
    {
        Underlying_->HandleStreamingPayload(payload);
    }

    void HandleStreamingFeedback(const TStreamingFeedback& feedback) override
    {
        Underlying_->HandleStreamingFeedback(feedback);
    }

private:
    IClientResponseHandlerPtr Underlying_;
};

DEFINE_REFCOUNTED_TYPE(TClientResponseHandlerWrapper)

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_CLASS(TGroundChannelWrapper)

class TGroundChannelWrapper
    : public IChannel
{
public:
    explicit TGroundChannelWrapper(IChannelPtr underlying)
        : Underlying_(std::move(underlying))
    { }

    const std::string& GetEndpointDescription() const override
    {
        return Underlying_->GetEndpointDescription();
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Underlying_->GetEndpointAttributes();
    }

     IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        const TSendOptions& options) override
    {
        auto wrappedResponseHandler = New<TClientResponseHandlerWrapper>(std::move(responseHandler));
        return Underlying_->Send(std::move(request), std::move(wrappedResponseHandler), options);
    }

    void Terminate(const TError& error) override
    {
        Underlying_->Terminate(error);
    }

    DECLARE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

    int GetInflightRequestCount() override
    {
        return Underlying_->GetInflightRequestCount();
    }

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override
    {
        return Underlying_->GetChannelMemoryTracker();
    }

private:
    IChannelPtr Underlying_;
};

DELEGATE_SIGNAL(TGroundChannelWrapper, void(const TError&), Terminated, *Underlying_);

DEFINE_REFCOUNTED_TYPE(TGroundChannelWrapper)

////////////////////////////////////////////////////////////////////////////////

IChannelPtr WrapGroundChannel(IChannelPtr underlyingChannel)
{
    return New<TGroundChannelWrapper>(std::move(underlyingChannel));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NSequoiaClient
