#include "stdafx.h"
#include "retrying_channel.h"
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

static auto& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannel
    : public IChannel
{
public:
    TRetryingChannel(
        TRetryingChannelConfigPtr config,
        IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const override;
    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override;

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout) override;

    virtual TFuture<void> Terminate(const TError& error) override;

private:
    IChannelPtr UnderlyingChannel;
    TRetryingChannelConfigPtr Config;

};

IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel)
{
    return New<TRetryingChannel>(config, underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

class TRetryingRequest
    : public IClientResponseHandler
{
public:
    TRetryingRequest(
        TRetryingChannelConfigPtr config,
        IChannelPtr underlyingChannel,
        IClientRequestPtr request,
        IClientResponseHandlerPtr originalHandler,
        TNullable<TDuration> timeout)
        : Config(std::move(config))
        , UnderlyingChannel(std::move(underlyingChannel))
        , CurrentAttempt(1)
        , Request(std::move(request))
        , OriginalHandler(std::move(originalHandler))
        , Timeout(timeout)
    {
        YASSERT(Config);
        YASSERT(UnderlyingChannel);
        YASSERT(Request);
        YASSERT(OriginalHandler);

        YCHECK(!Request->IsOneWay());

        Deadline = Config->RetryTimeout
            ? TInstant::Now() + *Config->RetryTimeout
            : TInstant::Max();
    }

    void Send()
    {
        LOG_DEBUG("Request attempt started (RequestId: %s, Attempt: %d of %d, RequestTimeout: %s, RetryTimeout: %s)",
            ~ToString(Request->GetRequestId()),
            static_cast<int>(CurrentAttempt),
            Config->RetryAttempts,
            ~ToString(Timeout),
            ~ToString(Config->RetryTimeout));

        auto now = TInstant::Now();
        if (now > Deadline) {
            ReportError(TError(NRpc::EErrorCode::Timeout, "Request retries timed out"));
            return;
        }

        auto timeout = ComputeAttemptTimeout(now);
        UnderlyingChannel->Send(Request, this, timeout);
    }

private:
    TRetryingChannelConfigPtr Config;
    IChannelPtr UnderlyingChannel;

    //! The current attempt number (1-based).
    int CurrentAttempt;
    IClientRequestPtr Request;
    IClientResponseHandlerPtr OriginalHandler;
    TNullable<TDuration> Timeout;
    TInstant Deadline;
    std::vector<TError> InnerErrors;


    // IClientResponseHandler implementation.

    virtual void OnAcknowledgement() override
    {
        LOG_DEBUG("Request attempt acknowledged (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        // NB: OriginalHandler is not notified.
    }

    virtual void OnError(const TError& error) override
    {
        LOG_DEBUG(error, "Request attempt failed (RequestId: %s, Attempt: %d of %d)",
            ~ToString(Request->GetRequestId()),
            static_cast<int>(CurrentAttempt),
            Config->RetryAttempts);

        if (!IsRetriableError(error)) {
            OriginalHandler->OnError(error);
            return;
        }

        InnerErrors.push_back(error);
        Retry();
    }

    virtual void OnResponse(TSharedRefArray message) override
    {
        LOG_DEBUG("Request attempt succeeded (RequestId: %s)",
            ~ToString(Request->GetRequestId()));

        auto this_ = MakeStrong(this);
        OriginalHandler->OnResponse(message);
    }


    TNullable<TDuration> ComputeAttemptTimeout(TInstant now)
    {
        auto attemptDeadline = Timeout ? now + *Timeout : TInstant::Max();
        auto actualDeadline = std::min(Deadline, attemptDeadline);
        return actualDeadline == TInstant::Max()
            ? TNullable<TDuration>(Null)
            : actualDeadline - now;
    }

    void ReportError(TError error)
    {
        error.InnerErrors() = InnerErrors;
        OriginalHandler->OnError(error);
    }

    void Retry()
    {
        int count = ++CurrentAttempt;
        if (count > Config->RetryAttempts || TInstant::Now() + Config->RetryBackoffTime > Deadline) {
            ReportError(TError(NRpc::EErrorCode::Unavailable, "Request retries failed"));
            return;
        }

        TDelayedExecutor::Submit(
            BIND(&TRetryingRequest::Send, MakeStrong(this)),
            Config->RetryBackoffTime);
    }
};

////////////////////////////////////////////////////////////////////////////////

TRetryingChannel::TRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel)
    : UnderlyingChannel(underlyingChannel)
    , Config(config)
{
    YCHECK(config);
    YCHECK(underlyingChannel);
}

void TRetryingChannel::Send(
    IClientRequestPtr request,
    IClientResponseHandlerPtr responseHandler,
    TNullable<TDuration> timeout)
{
    YASSERT(request);
    YASSERT(responseHandler);

    New<TRetryingRequest>(
        Config,
        UnderlyingChannel,
        request,
        responseHandler,
        timeout)
    ->Send();
}

TFuture<void> TRetryingChannel::Terminate(const TError& error)
{
    return UnderlyingChannel->Terminate(error);
}

TNullable<TDuration> TRetryingChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel->GetDefaultTimeout();
}

void TRetryingChannel::SetDefaultTimeout(const TNullable<TDuration>& timeout)
{
    UnderlyingChannel->SetDefaultTimeout(timeout);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
