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
    TRetryingChannel(
        TRetryConfigPtr config,
        IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const;

    virtual void Send(
        IClientRequestPtr request, 
        IClientResponseHandlerPtr responseHandler, 
        TNullable<TDuration> timeout);

    virtual void Terminate(const TError& error);

private:
    TRetryConfigPtr Config;
    IChannelPtr UnderlyingChannel;

};

IChannelPtr CreateRetryingChannel(
    TRetryConfigPtr config,
    IChannelPtr underlyingChannel)
{
    return New<TRetryingChannel>(
        config,
        underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

class TRetryingRequest
    : public IClientResponseHandler
{
public:
    TRetryingRequest(
        TRetryConfigPtr config,
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
            UnderlyingChannel->Send(Request, this, Deadline - now);
        } else {
            ReportUnavailable();
        }
    }

private:
    TRetryConfigPtr Config;
    IChannelPtr UnderlyingChannel;

    //! The current attempt number (1-based).
    TAtomic CurrentAttempt;
    IClientRequestPtr Request;
    IClientResponseHandlerPtr OriginalHandler;
    TNullable<TDuration> Timeout;
    TInstant Deadline;
    Stroka CumulativeErrorMessage;

    DECLARE_ENUM(EState, 
        (Sent)
        (Acked)
        (Done)
    );

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;

    virtual void OnAcknowledgement()
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

    virtual void OnError(const TError& error) 
    {
        LOG_DEBUG("Request attempt failed (RequestId: %s, Attempt: %d of %d)\n%s",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt),
            Config->MaxAttempts,
            ~error.ToString());

        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done)
            return;

        if (IsRetriableError(error)) {
            int count = AtomicIncrement(CurrentAttempt);

            CumulativeErrorMessage.append(Sprintf("\n[#%d] %s",
                count - 1,
                ~error.ToString()));

            if (count <= Config->MaxAttempts && TInstant::Now() + Config->BackoffTime < Deadline) {
                TDelayedInvoker::Submit(
                    BIND(&TRetryingRequest::Send, MakeStrong(this)),
                    Config->BackoffTime);
            } else {
                State = EState::Done;
                guard.Release();
                ReportUnavailable();
            }
        } else {
            State = EState::Done;
            guard.Release();

            OriginalHandler->OnError(error);
        }
    }

    virtual void OnResponse(IMessagePtr message)
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

    void ReportUnavailable()
    {
        OriginalHandler->OnError(TError(
            EErrorCode::Unavailable,
            "All retries have failed:" + CumulativeErrorMessage));
    }
};

////////////////////////////////////////////////////////////////////////////////

TRetryingChannel::TRetryingChannel(
    TRetryConfigPtr config,
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

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
