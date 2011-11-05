#include "stdafx.h"
#include "retry.h"

#include "../bus/bus_client.h"
#include "../misc/assert.h"

#include <util/system/spinlock.h>
#include <util/system/guard.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

//! Wraps an original request and provides a fresh request id.
class TRequestWrapper
    : public IClientRequest
{
public:
    TRequestWrapper(IClientRequest* originalRequest)
        : RequestId(TRequestId::Create())
        , OriginalRequest(originalRequest)
    { }

    virtual TRequestId GetRequestId() const
    {
        return RequestId;
    }

    IMessage::TPtr Serialize() const
    {
        return OriginalRequest->Serialize();
    }

    Stroka GetServiceName() const
    {
        return OriginalRequest->GetServiceName();
    }

    virtual Stroka GetMethodName() const
    {
        return OriginalRequest->GetMethodName();
    }

private:
    const TRequestId RequestId;
    IClientRequest::TPtr OriginalRequest;

};

////////////////////////////////////////////////////////////////////////////////

class TRetriableRequest
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TRetriableRequest> TPtr;

    TRetriableRequest(
        TRetriableChannel* channel,
        IClientRequest* originalRequest,
        IClientResponseHandler* originalResponseHandler,
        TDuration timeout)
        : CurrentAttempt(0)
        , Channel(channel)
        , OriginalRequest(originalRequest)
        , OriginalResponseHandler(originalResponseHandler)
        , Timeout(timeout)
        , SendResult(New< TFuture<EErrorCode> >())
    {
        YASSERT(channel != NULL);
        YASSERT(originalRequest != NULL);
        YASSERT(originalResponseHandler != NULL);
    }

    TFuture<EErrorCode>::TPtr Send() 
    {
        DoSend();
        return SendResult;
    }

private:
    //! The current attempt number (starting from 0).
    TAtomic CurrentAttempt;
    TRetriableChannel::TPtr Channel;
    IClientRequest::TPtr OriginalRequest;
    IClientResponseHandler::TPtr OriginalResponseHandler;
    TDuration Timeout;
    //! Result returned by #IChannel::Send
    TFuture<EErrorCode>::TPtr SendResult;

    DECLARE_ENUM(EState, 
        (Sent)
        (Acked)
        (Done)
    );

    //! Protects state transitions.
    TSpinLock SpinLock;
    EState State;

    void DoSend()
    {
        auto request = New<TRequestWrapper>(~OriginalRequest);
        Channel->GetUnderlyingChannel()->Send(
            request,
            this,
            Timeout);
    }

    virtual void OnAcknowledgement(IBus::ESendResult sendResult)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Sent) {
            switch (sendResult) {
                case IBus::ESendResult::OK:
                    State = EState::Acked;
                    guard.Release();

                    OriginalResponseHandler->OnAcknowledgement(sendResult);
                    break;

                case IBus::ESendResult::Failed:
                    State = EState::Done;
                    guard.Release();

                    OnAttemptFailed();
                    break;

                default:
                    YUNREACHABLE();
            }
        }
    }

    virtual void OnTimeout() 
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Sent) {
            State = EState::Done;
            guard.Release();

            OnAttemptFailed();
        }
    }

    virtual void OnResponse(EErrorCode errorCode, IMessage::TPtr message)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Sent) {
            State = EState::Done;
            guard.Release();

            if (!errorCode.IsRpcError()) {
                OriginalResponseHandler->OnResponse(errorCode, message);
                SendResult->Set(errorCode);
            } else {
                OnAttemptFailed();
            }
        }
    }


    void OnAttemptFailed()
    {
        int count = AtomicIncrement(CurrentAttempt);
        if (count < Channel->GetRetryCount()) {
            TDelayedInvoker::Get()->Submit(
                FromMethod(&TRetriableRequest::DoSend, TPtr(this)),
                TInstant::Now() + Channel->GetBackoffTime());
        } else {
            OriginalResponseHandler->OnResponse(EErrorCode::Unavailable, NULL);
            SendResult->Set(EErrorCode::Unavailable);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TRetriableChannel::TRetriableChannel(
    IChannel* underlyingChannel, 
    TDuration backoffTime, 
    int retryCount)
    : UnderlyingChannel_(underlyingChannel)
    , RetryCount_(retryCount)
    , BackoffTime_(backoffTime)
{
    YASSERT(underlyingChannel != NULL);
    YASSERT(retryCount >= 1);
}

TFuture<EErrorCode>::TPtr TRetriableChannel::Send(
    IClientRequest::TPtr request, 
    IClientResponseHandler::TPtr responseHandler, 
    TDuration timeout)
{
    YASSERT(~request != NULL);
    YASSERT(~responseHandler != NULL);

    auto retriableRequest = New<TRetriableRequest>(
        this,
        ~request,
        ~responseHandler,
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
