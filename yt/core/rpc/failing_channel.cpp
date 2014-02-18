#include "stdafx.h"
#include "failing_channel.h"

#include <core/rpc/channel.h>
#include <core/rpc/client.h>
#include <core/concurrency/fiber.h>

namespace NYT {
namespace NRpc {

using namespace NConcurrency;

DEFINE_REFCOUNTED_TYPE(IFailureModel)
DEFINE_REFCOUNTED_TYPE(TBlackHoleFailureModel)

class TEmptyResponseHandler
    : public IClientResponseHandler
{
public:
    TEmptyResponseHandler(IClientResponseHandlerPtr responseHandler, TDuration timeout)
        : Timeout_(timeout)
        , ResponseHandler_(responseHandler)
    { }

private:
    virtual void OnAcknowledgement()
    {
        ResponseHandler_->OnAcknowledgement();
    }

    virtual void OnResponse(TSharedRefArray message)
    {
        WaitFor(MakeDelayed(Timeout_));
        auto error = TError(
            EErrorCode::Timeout,
            "No response: this link is disabled");
        ResponseHandler_->OnError(error);
    }

    virtual void OnError(const TError& error)
    {
        ResponseHandler_->OnError(error);
    }

    TDuration Timeout_;
    IClientResponseHandlerPtr ResponseHandler_;
};

class TFailingChannel
    : public IChannel
{
public:
    explicit TFailingChannel(IChannelPtr underlyingChannel, IFailureModelPtr failureModel)
        : UnderlyingChannel_(underlyingChannel)
        , FailureModel_(failureModel)
    { }

    virtual TNullable<TDuration> GetDefaultTimeout() const override
    {
        return UnderlyingChannel_->GetDefaultTimeout();
    }

    virtual void SetDefaultTimeout(const TNullable<TDuration>& timeout) override
    {
        return UnderlyingChannel_->SetDefaultTimeout(timeout);
    }

    virtual void Send(
        IClientRequestPtr request,
        IClientResponseHandlerPtr responseHandler,
        TNullable<TDuration> timeout,
        bool requestAck) override
    {
        auto actualTimeout = timeout ? timeout.Get() : UnderlyingChannel_->GetDefaultTimeout().Get();
        if (FailureModel_->IsRequestFailing()) {
            WaitFor(MakeDelayed(actualTimeout));
            auto error = TError(
                EErrorCode::Timeout,
                "No request: this link is disabled");
            responseHandler->OnError(error);
        } else {
            if (FailureModel_->IsResponseFailing()) {
                UnderlyingChannel_->Send(request, New<TEmptyResponseHandler>(responseHandler, actualTimeout), timeout, requestAck);
            } else {
                UnderlyingChannel_->Send(request, responseHandler, timeout, requestAck);
            }
        }
    }

    virtual TFuture<void> Terminate(const TError& error) override
    {
        return UnderlyingChannel_->Terminate(error);
    }

private:
    IChannelPtr UnderlyingChannel_;
    IFailureModelPtr FailureModel_;
};

DEFINE_REFCOUNTED_TYPE(TFailingChannel)

IChannelPtr CreateFailingChannel(
    IChannelPtr underlyingChannel,
    IFailureModelPtr failureModel)
{
    return New<TFailingChannel>(underlyingChannel, failureModel);
}

} // namespace NYT
} // namespace NRpc
