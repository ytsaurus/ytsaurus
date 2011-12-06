#pragma once

#include "common.h"

#include "bus.h"
#include "message.h"

#include "../misc/config.h"

#include <quality/NetLiba/UdpHttp.h>
#include <quality/NetLiba/UdpAddress.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////


//! A client IBus factory.
/*!
 *  Thread affinity: any.
 */
struct IBusClient
    : virtual TRefCountedBase
{
public:
    typedef TIntrusivePtr<IBusClient> TPtr;

    //! Creates a new bus.
    /*!
     *  The bus will point to the address supplied during construction.
     *
     *  \param handler A handler that will process incoming messages.
     *  \return A new bus.
     *
     */
    virtual IBus::TPtr CreateBus(IMessageHandler* handler) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO: place into a separate file (nl_bus_client.h/cpp?)

struct TNLBusClientConfig
    : TConfigBase
{
    Stroka Address;
    // TODO: move here MaxNLCallsPerIteration, ClientSleepQuantum, MessageRearrangeTimeout;

    explicit TNLBusClientConfig(const Stroka& address)
        : Address(address)
    { }
};

IBusClient::TPtr CreateNLBusClient(const TNLBusClientConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
