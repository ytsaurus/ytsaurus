#include "stdafx.h"
#include "retrying_channel.h"
#include "private.h"
#include "client.h"

#include <ytlib/bus/client.h>

#include <util/system/spinlock.h>
#include <util/system/guard.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcClientLogger;

////////////////////////////////////////////////////////////////////////////////

class TRetryingChannel
    : public IChannel
{
public:
    TRetryingChannel(TRetryingChannelConfigPtr config, IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const override;

    virtual bool GetRetryEnabled() const override;

    virtual void Send(
        IClientRequestPtr request, 
        IClientResponseHandlerPtr responseHandler, 
        TNullable<TDuration> timeout) override;

    virtual void Terminate(const TError& error) override;

private:
    TRetryingChannelConfigPtr Config;
    IChannelPtr UnderlyingChannel;

};

IChannelPtr CreateRetryingChannel(
    TRetryingChannelConfigPtr config,
    IChannelPtr underlyingChannel)
{
    return config->MaxAttempts == 1
        ? underlyingChannel 
        : New<TRetryingChannel>(config, underlyingChannel);
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
        : Config(config)
        , UnderlyingChannel(underlyingChannel)
        , CurrentAttempt(1)
        , Request(request)
        , OriginalHandler(originalHandler)
        , Timeout(timeout)
    {
        YASSERT(config);
        YASSERT(underlyingChannel);
        YASSERT(request);
        YASSERT(originalHandler);

        Deadline = Timeout ? TInstant::Now() + Timeout.Get() : TInstant::Max();
    }

    void Send() 
    {
        LOG_DEBUG("Request attempt started (RequestId: %s, Attempt: %d of %d)",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt),
            Config->MaxAttempts);

        auto now = TInstant::Now();
        if (now < Deadline) {
            ReportError(TError("Request retries timed out"));
        } else {
            UnderlyingChannel->Send(Request, this, Deadline - now);
        }
    }

private:
    TRetryingChannelConfigPtr Config;
    IChannelPtr UnderlyingChannel;

    //! The current attempt number (1-based).
    std::vector<TError> InnerErrors;
    TAtomic CurrentAttempt;
    IClientRequestPtr Request;
    IClientResponseHandlerPtr OriginalHandler;
    TNullable<TDuration> Timeout;
    TInstant Deadline;

    DECLARE_ENUM(EState, 
        (Sent)
        (Acked)
        (Done)
    );

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;

    virtual void OnAcknowledgement() override
    {
        LOG_DEBUG("Request attempt acknowledged (RequestId: %s)",
            ~Request->GetRequestId().ToString());
        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Sent)
                return;
            State = EState::Acked;
        }

        OriginalHandler->OnAcknowledgement();
    }

    virtual void OnError(const TError& error) override
    {
        LOG_DEBUG("Request attempt failed (RequestId: %s, Attempt: %d of %d)\n%s",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt),
            Config->MaxAttempts,
            ~ToString(error));

        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done)
            return;

        if (IsRetriableError(error)) {
            int count = AtomicIncrement(CurrentAttempt);

            InnerErrors.push_back(error);

            if (count <= Config->MaxAttempts && TInstant::Now() + Config->BackoffTime < Deadline) {
                TDelayedInvoker::Submit(
                    BIND(&TRetryingRequest::Send, MakeStrong(this)),
                    Config->BackoffTime);
            } else {
                State = EState::Done;
                guard.Release();
                ReportError(TError("Request retries failed"));
            }
        } else {
            State = EState::Done;
            guard.Release();

            OriginalHandler->OnError(error);
        }
    }

    virtual void OnResponse(IMessagePtr message) override
    {
        LOG_DEBUG("Request attempt succeeded (RequestId: %s)",
            ~Request->GetRequestId().ToString());

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Sent && State != EState::Acked)
                return;
            State = EState::Done;
        }

        OriginalHandler->OnResponse(message);
    }

    void ReportError(TError error)
    {
        error.InnerErrors() = InnerErrors;
        OriginalHandler->OnError(error);
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

void TRetryingChannel::Terminate(const TError& error)
{
    UnderlyingChannel->Terminate(error);
}

TNullable<TDuration> TRetryingChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel->GetDefaultTimeout();
}

bool TRetryingChannel::GetRetryEnabled() const
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
