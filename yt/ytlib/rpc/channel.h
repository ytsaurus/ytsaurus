#pragma once

#include "common.h"

#include "../bus/bus_client.h"

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
     */
    virtual void Send(
        IClientRequest* request,
        IClientResponseHandler* responseHandler,
        TDuration timeout) = 0;

    //! Shuts down the channel.
    /*!
     *  It is safe to call this method multiple times.
     *  After the first call the instance is no longer usable.
     */
    virtual void Terminate() = 0;
};

//! Creates a channel implemented via NBus.
IChannel::TPtr CreateBusChannel(NBus::TBusClient* client);

//! Creates a channel implemented via NBus.
IChannel::TPtr CreateBusChannel(const Stroka& address);

////////////////////////////////////////////////////////////////////////////////

} // namespace NRpc
} // namespace NYT
