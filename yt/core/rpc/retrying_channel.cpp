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
    TRetryingChannel(TRetryingChannelConfigPtr config, IChannelPtr underlyingChannel)
        : TChannelWrapper(std::move(underlyingChannel))
        , Config_(std::move(config))
    {
        YCHECK(Config_);
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        YASSERT(request);
        YASSERT(responseHandler);

        New<TRetryingRequest>(
            Config_,
            UnderlyingChannel_,
            request,
            responseHandler,
            timeout,
            requestAck)
        ->Send();
    }


private:
    class TRetryingRequest
        : public IClientResponseHandler
    {
    public:
        TRetryingRequest(
            TRetryingChannelConfigPtr config,
            IChannelPtr underlyingChannel,
            IClientRequestPtr request,
            IClientResponseHandlerPtr originalHandler,
            TNullable<TDuration> timeout,
            bool requestAck)
            : Config_(std::move(config))
            , UnderlyingChannel_(std::move(underlyingChannel))
            , CurrentAttempt_(1)
            , Request_(std::move(request))
            , OriginalHandler_(std::move(originalHandler))
            , Timeout_(timeout)
            , RequestAck_(requestAck)
        {
            YASSERT(Config_);
            YASSERT(UnderlyingChannel_);
            YASSERT(Request_);
            YASSERT(OriginalHandler_);

            YCHECK(!Request_->IsOneWay());

            Deadline_ = Config_->RetryTimeout
                ? TInstant::Now() + *Config_->RetryTimeout
                : TInstant::Max();
        }

        void Send()
        {
            LOG_DEBUG("Request attempt started (RequestId: %s, Attempt: %d of %d, RequestTimeout: %s, RetryTimeout: %s)",
                ~ToString(Request_->GetRequestId()),
                static_cast<int>(CurrentAttempt_),
                Config_->RetryAttempts,
                ~ToString(Timeout_),
                ~ToString(Config_->RetryTimeout));

            auto now = TInstant::Now();
            if (now > Deadline_) {
                ReportError(TError(NRpc::EErrorCode::Timeout, "Request retries timed out"));
                return;
            }

            auto timeout = ComputeAttemptTimeout(now);
            UnderlyingChannel_->Send(
                Request_,
                this,
                timeout,
                RequestAck_);
        }

    private:
        TRetryingChannelConfigPtr Config_;
        IChannelPtr UnderlyingChannel_;

        //! The current attempt number (1-based).
        int CurrentAttempt_;
        IClientRequestPtr Request_;
        IClientResponseHandlerPtr OriginalHandler_;
        TNullable<TDuration> Timeout_;
        bool RequestAck_;
        TInstant Deadline_;
        std::vector<TError> InnerErrors_;


        // IClientResponseHandler implementation.

        virtual void OnAcknowledgement() override
        {
            LOG_DEBUG("Request attempt acknowledged (RequestId: %s)",
                ~ToString(Request_->GetRequestId()));

            // NB: OriginalHandler is not notified.
        }

        virtual void OnError(const TError& error) override
        {
            LOG_DEBUG(error, "Request attempt failed (RequestId: %s, Attempt: %d of %d)",
                ~ToString(Request_->GetRequestId()),
                static_cast<int>(CurrentAttempt_),
                Config_->RetryAttempts);

            if (!IsRetriableError(error)) {
                OriginalHandler_->OnError(error);
                return;
            }

            InnerErrors_.push_back(error);
            Retry();
        }

        virtual void OnResponse(TSharedRefArray message) override
        {
            LOG_DEBUG("Request attempt succeeded (RequestId: %s)",
                ~ToString(Request_->GetRequestId()));

            OriginalHandler_->OnResponse(message);
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
            OriginalHandler_->OnError(detailedError);
        }

        void Retry()
        {
            int count = ++CurrentAttempt_;
            if (count > Config_->RetryAttempts || TInstant::Now() + Config_->RetryBackoffTime > Deadline_) {
                ReportError(TError(NRpc::EErrorCode::Unavailable, "Request retries failed"));
                return;
            }

            TDelayedExecutor::Submit(
                BIND(&TRetryingRequest::Send, MakeStrong(this)),
                Config_->RetryBackoffTime);
        }
    };

    TRetryingChannelConfigPtr Config_;

};

IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel)
{
    return New<TRetryingChannel>(config, underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
