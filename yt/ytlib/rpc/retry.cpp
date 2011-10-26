#include "stdafx.h"
#include "retry.h"

#include "../bus/bus_client.h"
#include "../misc/assert.h"

#include <util/system/spinlock.h>

namespace NYT {
namespace NRpc {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

class TRetryResponseHandler
    : public IClientResponseHandler
{
public:
    typedef TIntrusivePtr<TRetryResponseHandler> TPtr;

    TRetryResponseHandler(
        IClientResponseHandler::TPtr responseHandler,
        int retryCount)
        : responseHandler(responseHandler)
    { }

    void OnAcknowledgement(IBus::ESendResult sendResult) 
    {
        switch (sendResult) {
        case IBus::ESendResult::OK:
            
            break;

        case IBus::ESendResult::Failed:
            break;

        default:
            YUNREACHABLE();
        }
    }

    void OnResponse(EErrorCode errorCode, NBus::IMessage::TPtr message) 
    {
    }

    void OnTimeout() 
    {
    }

private:
    IClientResponseHandler::TPtr responseHandler
    TSpinLock SpinLock;

};

////////////////////////////////////////////////////////////////////////////////

TRetriableChannel::TRetriableChannel(
    IChannel::TPtr channel, 
    TDuration backoffTime, 
    int retryCount)
    : Channel(channel)
    , BackoffTime(backoffTime)
    , RetryCount(retryCount)
{ }

TFuture<EErrorCode>::TPtr Send(
    TClientRequest::TPtr request, 
    IClientResponseHandler::TPtr responseHandler, 
    TDuration timeout)
{
    Channel->Send(request, 
}

void Terminate()
{

}

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT