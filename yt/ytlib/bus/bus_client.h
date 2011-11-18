#pragma once

#include "common.h"

#include "bus.h"
#include "message.h"

#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TClientDispatcher;

//! A client IBus factory.
/*!
 *  Thread affinity: any.
 */
class TBusClient
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TBusClient> TPtr;

    //! Initializes a new client for communicating with a given address.
    /*!
     *  DNS resolution is performed upon construction, the resulting
     *  IP address is cached.
     *
     *  \param address An address where all buses will point to.
     */
    TBusClient(const Stroka& address);

    //! Creates a new bus.
    /*!
     *  The bus will point to the address supplied during construction.
     *  
     *  \param handler A handler that will process incoming messages.
     *  \return A new bus.
     *
     */
    IBus::TPtr CreateBus(IMessageHandler::TPtr handler);

private:
    class TBus;
    friend class TClientDispatcher;

    TUdpAddress ServerAddress;

};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
