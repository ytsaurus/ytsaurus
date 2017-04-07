#include "roaming_channel.h"
#include "channel_detail.h"
#include "client.h"

#include <yt/core/actions/future.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TRoamingRequestControl
    : public TClientRequestControlThunk
{
public:
    TRoamingRequestControl(
        TFuture<IChannelPtr> asyncChannel,
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck)
        : Request_(std::move(request))
        , ResponseHandler_(std::move(responseHandler))
        , Timeout_(timeout)
        , RequestAck_(requestAck)
        , StartTime_(TInstant::Now())
    {
        if (Timeout_) {
            asyncChannel = asyncChannel.WithTimeout(*Timeout_);
        }

        asyncChannel.Subscribe(BIND(&TRoamingRequestControl::OnGotChannel, MakeStrong(this)));
    }

    virtual void Cancel() override
    {
        if (!TryAcquireSemaphore()) {
            TClientRequestControlThunk::Cancel();
            return;
        }

        auto error = TError(NYT::EErrorCode::Canceled, "RPC request canceled")
            << TErrorAttribute("request_id", Request_->GetRequestId())
            << TErrorAttribute("service", Request_->GetService())
            << TErrorAttribute("method", Request_->GetMethod());
        ResponseHandler_->HandleError(error);

        Request_.Reset();
        ResponseHandler_.Reset();
    }

private:
    IClientRequestPtr Request_;
    IClientResponseHandlerPtr ResponseHandler_;
    const TNullable<TDuration> Timeout_;
    const bool RequestAck_;
    const TInstant StartTime_;

    std::atomic<bool> Semaphore_ = {false};


    bool TryAcquireSemaphore()
    {
        bool expected = false;
        return Semaphore_.compare_exchange_strong(expected, true);
    }

    void OnGotChannel(const TErrorOr<IChannelPtr>& result)
    {
        if (!TryAcquireSemaphore()) {
            return;
        }

        if (!result.IsOK()) {
            ResponseHandler_->HandleError(result);
            return;
        }

        TNullable<TDuration> adjustedTimeout;
        if (Timeout_) {
            auto now = TInstant::Now();
            auto deadline = StartTime_ + *Timeout_;
            adjustedTimeout = now > deadline ? TDuration::Zero() : deadline - now;
        }

        const auto& channel = result.Value();
        auto requestControl = channel->Send(
            std::move(Request_),
            std::move(ResponseHandler_),
            adjustedTimeout,
            RequestAck_);

        SetUnderlying(std::move(requestControl));
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRoamingChannel
    : public IChannel
{
public:
    explicit TRoamingChannel(IRoamingChannelProviderPtr provider)
        : Provider_(std::move(provider))
    { }

    virtual const Stroka& GetEndpointDescription() const override
    {
        return Provider_->GetEndpointDescription();
    }

    virtual const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Provider_->GetEndpointAttributes();
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        Y_ASSERT(request);
        Y_ASSERT(responseHandler);

        auto asyncChannel = Provider_->GetChannel(request);

        // NB: Optimize for the typical case of sync channel acquisition.
        auto channelOrError = asyncChannel.TryGet();
        if (channelOrError) {
            if (channelOrError->IsOK()) {
                const auto& channel = channelOrError->Value();
                return channel->Send(
                    std::move(request),
                    std::move(responseHandler),
                    timeout,
                    requestAck);
            } else {
                responseHandler->HandleError(*channelOrError);
                return New<TClientRequestControlThunk>();
            }
        }

        return New<TRoamingRequestControl>(
            std::move(asyncChannel),
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck);
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return Provider_->Terminate(error);
    }

private:
    const IRoamingChannelProviderPtr Provider_;

};

IChannelPtr CreateRoamingChannel(IRoamingChannelProviderPtr provider)
{
    YCHECK(provider);

    return New<TRoamingChannel>(std::move(provider));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
