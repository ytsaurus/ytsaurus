#pragma once

#include "common.h"

#include "../bus/bus_client.h"
#include "../actions/future.h"
#include "../misc/delayed_invoker.h"

namespace NYT {
namespace NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IClientRequest;
struct IClientResponseHandler;

/*!
 * \note Thread affinity: any.
 */
struct IChannel
    : virtual TRefCountedBase
{
    typedef TIntrusivePtr<IChannel> TPtr;

    //! Sends a request via the channel.
    /*!
     *  \param request A request to send.
     *  \param responseHandler An object that will handle a response (if any).
     *  \param timeout Request processing timeout.
     *  \return An asynchronous result of an RPC call.
     */
    virtual TFuture<TError>::TPtr Send(
        TIntrusivePtr<IClientRequest> request,
        TIntrusivePtr<IClientResponseHandler> responseHandler,
        TDuration timeout) = 0;

    //! Shuts down the channel.
    /*!
     *  It is safe to call this method multiple times.
     *  After the first call the instance is no longer usable.
     */
    virtual void Terminate() = 0;
};

////////////////////////////////////////////////////////////////////////////////

//! Implements IChannel via NYT::NBus.
class TChannel
    : public IChannel
    , public NBus::IMessageHandler
{
public:
    typedef TIntrusivePtr<TChannel> TPtr;

    TChannel(NBus::TBusClient::TPtr client);
    TChannel(Stroka address);

    virtual TFuture<TError>::TPtr Send(
        TIntrusivePtr<IClientRequest> request,
        TIntrusivePtr<IClientResponseHandler> responseHandler,
        TDuration timeout);

    virtual void Terminate();

private:
    friend class TClientRequest;
    friend class TClientResponse;

    struct TActiveRequest
    {
        TRequestId RequestId;
        TIntrusivePtr<IClientResponseHandler> ResponseHandler;
        TFuture<TError>::TPtr Ready;
        TDelayedInvoker::TCookie TimeoutCookie;
    };

    typedef yhash_map<TRequestId, TActiveRequest> TRequestMap;

    volatile bool Terminated;
    NBus::IBus::TPtr Bus;
    TRequestMap ActiveRequests;
    //! Protects #ActiveRequests and #Terminated.
    TSpinLock SpinLock;

    void OnAcknowledgement(
        NBus::IBus::ESendResult sendResult,
        TRequestId requestId);

    virtual void OnMessage(
        NBus::IMessage::TPtr message,
        NBus::IBus::TPtr replyBus);

    void OnTimeout(TRequestId requestId);

    void UnregisterRequest(TRequestMap::iterator it);
};          

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
