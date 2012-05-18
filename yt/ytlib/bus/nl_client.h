#pragma once

#include "common.h"
#include "client.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TNLBusClientConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TNLBusClientConfig> TPtr;

    Stroka Address;
    // TODO: move here MaxNLCallsPerIteration, ClientSleepQuantum, MessageRearrangeTimeout;

    TNLBusClientConfig()
    {
        Register("address", Address);
    }

    explicit TNLBusClientConfig(const Stroka& address)
        : Address(address)
    { }
};

////////////////////////////////////////////////////////////////////////////////

//! Initializes a new client for communicating with a given address.
/*!
 *  DNS resolution is performed upon construction, the resulting
 *  IP address is cached.
 *
 *  \param address An address where all buses will point to.
 */
IBusClient::TPtr CreateNLBusClient(TNLBusClientConfig* config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
