#include "stdafx.h"
#include "retriable_channel.h"
#include "client.h"

#include <ytlib/bus/client.h>

#include <util/system/spinlock.h>
#include <util/system/guard.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = RpcLogger;

////////////////////////////////////////////////////////////////////////////////

class TRetriableChannel
    : public IChannel
{
    DEFINE_BYVAL_RO_PROPERTY(IChannelPtr, UnderlyingChannel);
    DEFINE_BYVAL_RO_PROPERTY(TRetryConfigPtr, Config);

public:
    TRetriableChannel(
        TRetryConfig* config,
        IChannelPtr underlyingChannel);

    virtual TNullable<TDuration> GetDefaultTimeout() const;

    virtual void Send(
        IClientRequestPtr request, 
        IClientResponseHandlerPtr responseHandler, 
        TNullable<TDuration> timeout);

    virtual void Terminate();

};

IChannelPtr CreateRetriableChannel(
    TRetryConfig* config,
    IChannelPtr underlyingChannel)
{
    return New<TRetriableChannel>(
        config,
        underlyingChannel);
}

////////////////////////////////////////////////////////////////////////////////

class TRetriableRequest
    : public IClientResponseHandler
{
public:
    TRetriableRequest(
        TRetriableChannel* channel,
        IClientRequestPtr request,
        IClientResponseHandlerPtr originalHandler,
        TNullable<TDuration> timeout)
        : CurrentAttempt(0)
        , Channel(channel)
        , Request(request)
        , OriginalHandler(originalHandler)
        , Timeout(timeout)
    {
        YASSERT(channel);
        YASSERT(request);
        YASSERT(originalHandler);

        Deadline = Timeout ? TInstant::Now() + Timeout.Get() : TInstant::Max();
    }

    void Send() 
    {
        LOG_DEBUG("Retriable request sent (RequestId: %s, Attempt: %d)",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt));

        auto now = TInstant::Now();
        if (now < Deadline) {
            Channel->GetUnderlyingChannel()->Send(
                ~Request,
                this,
                Deadline - now);
        } else {
            ReportUnavailable();
        }
    }

private:
    //! The current attempt number (starting from 0).
    TAtomic CurrentAttempt;
    TRetriableChannelPtr Channel;
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
        LOG_DEBUG("Retriable request acknowledged (RequestId: %s)",
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
        LOG_DEBUG("Retriable request attempt failed (RequestId: %s, Attempt: %d)\n%s",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt),
            ~error.ToString());

        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Done)
            return;

        if (IsRpcError(error)) {
            int count = AtomicIncrement(CurrentAttempt);

            CumulativeErrorMessage.append(Sprintf("\n[#%d] %s",
                count,
                ~error.ToString()));

            TDuration backoffTime = Channel->GetConfig()->BackoffTime;
            if (count < Channel->GetConfig()->RetryCount &&
                TInstant::Now() + backoffTime < Deadline)
            {
                TDelayedInvoker::Submit(
                    BIND(&TRetriableRequest::Send, MakeStrong(this)),
                    backoffTime);
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

    virtual void OnResponse(IMessage* message)
    {
        LOG_DEBUG("Retriable response received (RequestId: %s)",
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
            "Retriable request failed, details follow" + CumulativeErrorMessage));
    }
};

////////////////////////////////////////////////////////////////////////////////

TRetriableChannel::TRetriableChannel(
    TRetryConfig* config,
    IChannelPtr underlyingChannel)
    : UnderlyingChannel_(underlyingChannel)
    , Config_(config)
{
    YASSERT(underlyingChannel);
}

void TRetriableChannel::Send(
    IClientRequestPtr request, 
    IClientResponseHandlerPtr responseHandler, 
    TNullable<TDuration> timeout)
{
    YASSERT(request);
    YASSERT(responseHandler);

    auto retriableRequest = New<TRetriableRequest>(
        this,
        request,
        responseHandler,
        timeout);

    return retriableRequest->Send();
}

void TRetriableChannel::Terminate()
{
    UnderlyingChannel_->Terminate();
}

TNullable<TDuration> TRetriableChannel::GetDefaultTimeout() const
{
    return UnderlyingChannel_->GetDefaultTimeout();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
