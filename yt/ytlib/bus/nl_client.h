#pragma once

#include "common.h"
#include "client.h"

#include "../misc/config.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TNLBusClientConfig
    : TConfigBase
{
    typedef TIntrusivePtr<TNLBusClientConfig> TPtr;

    Stroka Address;
    // TODO: move here MaxNLCallsPerIteration, ClientSleepQuantum, MessageRearrangeTimeout;

    explicit TNLBusClientConfig(const Stroka& address)
        : Address(address)
    { }
};

////////////////////////////////////////////////////////////////////////////////

IBusClient::TPtr CreateNLBusClient(TNLBusClientConfig* config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
