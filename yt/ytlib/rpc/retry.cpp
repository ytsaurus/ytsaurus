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
        , SendResult(New< TFuture<TError> >())
    {
        YASSERT(channel != NULL);
        YASSERT(request != NULL);
        YASSERT(originalHandler != NULL);
    }

    TFuture<TError>::TPtr Send() 
    {
        DoSend();
        return SendResult;
    }

private:
    //! The current attempt number (starting from 0).
    TAtomic CurrentAttempt;
    TRetriableChannel::TPtr Channel;
    IClientRequest::TPtr Request;
    IClientResponseHandler::TPtr OriginalHandler;
    TDuration Timeout;
    //! Result returned by #IChannel::Send
    TFuture<TError>::TPtr SendResult;

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
        Channel->GetUnderlyingChannel()->Send(
            Request,
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

                    OriginalHandler->OnAcknowledgement(sendResult);
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

    virtual void OnResponse(const TError& error, IMessage* message)
    {
        TGuard<TSpinLock> guard(SpinLock);
        if (State == EState::Sent || State == EState::Acked) {
            State = EState::Done;
            guard.Release();

            if (!error.IsRpcError()) {
                OriginalHandler->OnResponse(error, message);
                SendResult->Set(error);
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
            // TODO: provide better diagnostics
            // TODO(sandello): We have to aggregate acquired errors and tell about them here.
            TError error(EErrorCode::Unavailable, "Request retrial has failed");
            OriginalHandler->OnResponse(error, NULL);
            SendResult->Set(error);
        }
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

TFuture<TError>::TPtr TRetriableChannel::Send(
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
