#include "stdafx.h"
#include "retry.h"
#include "client.h"

#include "../bus/bus_client.h"
#include "../misc/assert.h"

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
    DEFINE_BYVAL_RO_PROPERTY(IChannel::TPtr, UnderlyingChannel);
    DEFINE_BYVAL_RO_PROPERTY(TDuration, BackoffTime);
    DEFINE_BYVAL_RO_PROPERTY(int, RetryCount);

public:
    typedef TIntrusivePtr<TRetriableChannel> TPtr;

    TRetriableChannel(
        IChannel* underlyingChannel, 
        TDuration backoffTime, 
        int retryCount);

    void Send(
        IClientRequest* request, 
        IClientResponseHandler* responseHandler, 
        TDuration timeout);

    void Terminate();

};

IChannel::TPtr CreateRetriableChannel(
    IChannel* underlyingChannel,
    TDuration backoffTime,
    int retryCount)
{
    return New<TRetriableChannel>(
        underlyingChannel,
        backoffTime,
        retryCount);
}

////////////////////////////////////////////////////////////////////////////////

class TRetriableRequest
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TRetriableRequest> TPtr;

    TRetriableRequest(
        TRetriableChannel* channel,
        IClientRequest* request,
        IClientResponseHandler* originalHandler,
        TDuration timeout)
        : CurrentAttempt(0)
        , Channel(channel)
        , Request(request)
        , OriginalHandler(originalHandler)
        , Timeout(timeout)
    {
        YASSERT(channel != NULL);
        YASSERT(request != NULL);
        YASSERT(originalHandler != NULL);
    }

    void Send() 
    {
        LOG_DEBUG("Retriable request sent (RequestId: %s, Attempt: %d)",
            ~Request->GetRequestId().ToString(),
            static_cast<int>(CurrentAttempt));

        Channel->GetUnderlyingChannel()->Send(
            ~Request,
            this,
            Timeout);
    }

private:
    //! The current attempt number (starting from 0).
    TAtomic CurrentAttempt;
    TRetriableChannel::TPtr Channel;
    IClientRequest::TPtr Request;
    IClientResponseHandler::TPtr OriginalHandler;
    TDuration Timeout;
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
        LOG_DEBUG("Retriable request failed (RequestId: %s)\n%s",
            ~Request->GetRequestId().ToString(),
            ~error.ToString());

        {
            TGuard<TSpinLock> guard(SpinLock);
            if (State != EState::Sent)
                return;
            State = EState::Done;
        }

        if (error.IsRpcError()) {
            int count = AtomicIncrement(CurrentAttempt);

            CumulativeErrorMessage.append(Sprintf("\n[%d]: %s",
                count,
                ~error.ToString()));

            if (count < Channel->GetRetryCount()) {
                TDelayedInvoker::Get()->Submit(
                    FromMethod(&TRetriableRequest::Send, TPtr(this)),
                    TInstant::Now() + Channel->GetBackoffTime());
            } else {
                OriginalHandler->OnError(TError(
                    EErrorCode::Unavailable,
                    "Retriable request has failed, details follow:" + CumulativeErrorMessage));
            }
        } else {
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
};

////////////////////////////////////////////////////////////////////////////////

TRetriableChannel::TRetriableChannel(
    IChannel* underlyingChannel, 
    TDuration backoffTime, 
    int retryCount)
    : UnderlyingChannel_(underlyingChannel)
    , BackoffTime_(backoffTime)
    , RetryCount_(retryCount)
{
    YASSERT(underlyingChannel != NULL);
    YASSERT(retryCount >= 1);
}

void TRetriableChannel::Send(
    IClientRequest* request, 
    IClientResponseHandler* responseHandler, 
    TDuration timeout)
{
    YASSERT(request != NULL);
    YASSERT(responseHandler != NULL);

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

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
