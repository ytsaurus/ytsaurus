#include "stdafx.h"
#include "retrying_channel.h"
#include "channel_detail.h"
#include "private.h"
#include "client.h"
#include "config.h"

#include <core/bus/client.h>

#include <util/system/spinlock.h>
#include <util/system/guard.h>

namespace NYT {
namespace NRpc {

using namespace NBus;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannel
    : public TChannelWrapper
{
public:
    TRetryingChannel(
        TRetryingChannelConfigPtr config,
        IChannelPtr underlyingChannel,
        TCallback<bool(const TError&)> isRetriableError)
        : TChannelWrapper(std::move(underlyingChannel))
        , Config_(std::move(config))
        , IsRetriableError_(isRetriableError)
    {
        YCHECK(Config_);
    }

    virtual IClientRequestControlPtr Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        YASSERT(request);
        YASSERT(responseHandler);

        return New<TRetryingRequest>(
            Config_,
            UnderlyingChannel_,
            std::move(request),
            std::move(responseHandler),
            timeout,
            requestAck,
            IsRetriableError_)
        ->Send();
    }


private:
    const TRetryingChannelConfigPtr Config_;
    const TCallback<bool(const TError&)> IsRetriableError_;


    class TRetryingRequest
        : public IClientResponseHandler
    {
    public:
        TRetryingRequest(
            TRetryingChannelConfigPtr config,
            IChannelPtr underlyingChannel,
            IClientRequestPtr request,
            IClientResponseHandlerPtr responseHandler,
            TNullable<TDuration> timeout,
            bool requestAck,
            TCallback<bool(const TError&)> isRetriableError)
            : Config_(std::move(config))
            , UnderlyingChannel_(std::move(underlyingChannel))
            , Request_(std::move(request))
            , ResponseHandler_(std::move(responseHandler))
            , Timeout_(timeout)
            , RequestAck_(requestAck)
            , IsRetriableError_(std::move(isRetriableError))
        {
            YASSERT(Config_);
            YASSERT(UnderlyingChannel_);
            YASSERT(Request_);
            YASSERT(ResponseHandler_);

            YCHECK(!Request_->IsOneWay());

            Deadline_ = Config_->RetryTimeout
                ? TInstant::Now() + *Config_->RetryTimeout
                : TInstant::Max();
        }

        IClientRequestControlPtr Send()
        {
            DoSend();
            return RequestControlThunk_;
        }

    private:
        const TRetryingChannelConfigPtr Config_;
        const IChannelPtr UnderlyingChannel_;
        const IClientRequestPtr Request_;
        const IClientResponseHandlerPtr ResponseHandler_;
        const TNullable<TDuration> Timeout_;
        const bool RequestAck_;
        const TCallback<bool(const TError&)> IsRetriableError_;
        const TClientRequestControlThunkPtr RequestControlThunk_ = New<TClientRequestControlThunk>();

        //! The current attempt number (1-based).
        int CurrentAttempt_ = 1;
        TInstant Deadline_;
        std::vector<TError> InnerErrors_;

        // IClientResponseHandler implementation.

        virtual void HandleAcknowledgement() override
        {
            LOG_DEBUG("Request attempt acknowledged (RequestId: %v)",
                Request_->GetRequestId());

            // NB: The underlying handler is not notified.
        }

        virtual void HandleError(const TError& error) override
        {
            LOG_DEBUG(error, "Request attempt failed (RequestId: %v, Attempt: %v of %v)",
                Request_->GetRequestId(),
                CurrentAttempt_,
                Config_->RetryAttempts);

            if (!IsRetriableError_.Run(error)) {
                ResponseHandler_->HandleError(error);
                return;
            }

            InnerErrors_.push_back(error);
            Retry();
        }

        virtual void HandleResponse(TSharedRefArray message) override
        {
            LOG_DEBUG("Request attempt succeeded (RequestId: %v)",
                Request_->GetRequestId());

            ResponseHandler_->HandleResponse(message);
        }


        TNullable<TDuration> ComputeAttemptTimeout(TInstant now)
        {
            auto attemptDeadline = Timeout_ ? now + *Timeout_ : TInstant::Max();
            auto actualDeadline = std::min(Deadline_, attemptDeadline);
            return actualDeadline == TInstant::Max()
                ? TNullable<TDuration>(Null)
                : actualDeadline - now;
        }

        void ReportError(const TError& error)
        {
            auto detailedError = error
                << TErrorAttribute("endpoint", UnderlyingChannel_->GetEndpointDescription())
                << InnerErrors_;
            ResponseHandler_->HandleError(detailedError);
        }

        void Retry()
        {
            int count = ++CurrentAttempt_;
            if (count > Config_->RetryAttempts || TInstant::Now() + Config_->RetryBackoffTime > Deadline_) {
                ReportError(TError(NRpc::EErrorCode::Unavailable, "Request retries failed"));
                return;
            }

            TDelayedExecutor::Submit(
                BIND(&TRetryingRequest::DoSend, MakeStrong(this)),
                Config_->RetryBackoffTime);
        }

        void DoSend()
        {
            LOG_DEBUG("Request attempt started (RequestId: %v, Method: %v:%v, Attempt: %v of %v, RequestTimeout: %v, RetryTimeout: %v)",
                Request_->GetRequestId(),
                Request_->GetService(),
                Request_->GetMethod(),
                CurrentAttempt_,
                Config_->RetryAttempts,
                Timeout_,
                Config_->RetryTimeout);

            auto now = TInstant::Now();
            if (now > Deadline_) {
                ReportError(TError(NYT::EErrorCode::Timeout, "Request retries timed out"));
                return;
            }

            auto timeout = ComputeAttemptTimeout(now);
            auto requestControl = UnderlyingChannel_->Send(
                Request_,
                this,
                timeout,
                RequestAck_);
            RequestControlThunk_->SetUnderlying(std::move(requestControl));
        }

    };

};

IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel,
    TCallback<bool(const TError&)> isRetriableError)
{
    return New<TRetryingChannel>(config, underlyingChannel, isRetriableError);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
