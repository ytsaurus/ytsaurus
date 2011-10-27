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

class TRetryRequestWrapper
    : public IClientRequest
{
public:
    TRetryRequestWrapper(IClientRequest::TPtr originalRequest)
        : RequestId(TRequestId::Create())
        , OriginalRequest(originalRequest)
    { }

    virtual TRequestId GetRequestId() const
    {
        return RequestId;
    }

    NBus::IMessage::TPtr Serialize() const
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

// ToDo: consider a better name
class TRetriableRpcCall;

//! Although several handler routines could be called for a #TRetryResponseHandler
//! instance, error or success will be reported to TRetriableRpcCall only once.
class TRetryResponseHandler
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TRetryResponseHandler> TPtr;

    TRetryResponseHandler(TIntrusivePtr<TRetriableRpcCall> retriableCall);

    void OnAcknowledgement(IBus::ESendResult sendResult);
    void OnResponse(EErrorCode errorCode, NBus::IMessage::TPtr message);
    void OnTimeout();

private:
    DECLARE_ENUM(EState, 
        (Sent)
        (Done)
    );

    EState State;

    TIntrusivePtr<TRetriableRpcCall> RetriableCall;
    TSpinLock SpinLock;
};

////////////////////////////////////////////////////////////////////////////////

//! As far as no more than one handler is active at any moment,
//! no locking is required.
class TRetriableRpcCall
    : public virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<TRetriableRpcCall> TPtr;

    TRetriableRpcCall(
        TRetriableChannel::TPtr channel,
        IClientRequest::TPtr originalRequest,
        IClientResponseHandler::TPtr originalResponseHandler,
        TDuration timeout,
        TFuture<EErrorCode>::TPtr sendResult)
        : RetryCount(0)
        , Channel(channel)
        , OriginalRequest(originalRequest)
        , OriginalResponseHandler(originalResponseHandler)
        , Timeout(timeout)
        , SendResult(sendResult)
    {
        YASSERT(~channel != NULL);
        YASSERT(~originalRequest != NULL);
        YASSERT(~originalResponseHandler != NULL);
        YASSERT(~sendResult != NULL);

        Send();
    }

    void OnFail()
    {
        ++RetryCount;
        if (RetryCount < Channel->GetRetryCount()) {
            TDelayedInvoker::Get()->Submit(
                FromMethod(&TRetriableRpcCall::Send, TPtr(this)),
                TInstant::Now() + Channel->GetBackoffTime());
        } else {
            OriginalResponseHandler->OnResponse(EErrorCode::Unavailable, NULL);
            SendResult->Set(EErrorCode::Unavailable);
        }
    }

    void OnResponse(EErrorCode errorCode, NBus::IMessage::TPtr message) 
    {
        YASSERT(~message != NULL);

        if (errorCode.IsOK()) {
            OriginalResponseHandler->OnResponse(errorCode, message);
            SendResult->Set(errorCode);
        } else {
            OnFail();
        }
    }

private:
    void Send() 
    {
        auto request = New<TRetryRequestWrapper>(OriginalRequest);
        auto responseHandlers = New<TRetryResponseHandler>(this);

        Channel->GetChannel()->Send(
            request,
            responseHandlers,
            Timeout
        );
    }

    int RetryCount;
    TRetriableChannel::TPtr Channel;
    IClientRequest::TPtr OriginalRequest;
    IClientResponseHandler::TPtr OriginalResponseHandler;
    TDuration Timeout;

    //! Result, returned by #IChannel::Send
    TFuture<EErrorCode>::TPtr SendResult;
};

////////////////////////////////////////////////////////////////////////////////

TRetryResponseHandler::TRetryResponseHandler(
    TRetriableRpcCall::TPtr retriableCall)
    : State(EState::Sent)
    , RetriableCall(retriableCall)
{
    YASSERT(~retriableCall != NULL);
}

void TRetryResponseHandler::OnAcknowledgement(
    IBus::ESendResult sendResult)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (State == EState::Sent) {
        switch (sendResult) {
        case IBus::ESendResult::OK:
            break;

        case IBus::ESendResult::Failed:
            State = EState::Done;
            guard.Release();

            RetriableCall->OnFail();
            break;

        default:
            YUNREACHABLE();
        }
    }
}

void TRetryResponseHandler::OnResponse(
    EErrorCode errorCode, 
    NBus::IMessage::TPtr message)
{
    TGuard<TSpinLock> guard(SpinLock);
    if (State == EState::Sent) {
        State = EState::Done;
        guard.Release();

        RetriableCall->OnResponse(errorCode, message);
    }
}

void TRetryResponseHandler::OnTimeout() 
{
    TGuard<TSpinLock> guard(SpinLock);
    if (State == EState::Sent) {
        State = EState::Done;
        guard.Release();

        RetriableCall->OnFail();
    }
}

////////////////////////////////////////////////////////////////////////////////

TRetriableChannel::TRetriableChannel(
    IChannel::TPtr channel, 
    TDuration backoffTime, 
    int retryCount)
    : Channel(channel)
    , RetryCount(retryCount)
    , BackoffTime(backoffTime)
{
    YASSERT(~channel != NULL);
}

TFuture<EErrorCode>::TPtr TRetriableChannel::Send(
    IClientRequest::TPtr request, 
    IClientResponseHandler::TPtr responseHandler, 
    TDuration timeout)
{
    YASSERT(~request != NULL);
    YASSERT(~responseHandler != NULL);

    auto result = New< TFuture<EErrorCode> > ();
    auto retriableCall = New<TRetriableRpcCall>(
        this,
        request,
        responseHandler,
        timeout,
        result);

    return result;
}

IChannel::TPtr TRetriableChannel::GetChannel()
{
    return Channel;
}

void TRetriableChannel::Terminate()
{
    Channel->Terminate();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
