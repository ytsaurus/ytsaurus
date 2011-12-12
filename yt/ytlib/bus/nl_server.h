#pragma once

#include "common.h"
#include "server.h"

#include "../misc/config.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TNLBusServerConfig
    : TConfigBase
{
    int Port;
    int MaxNLCallsPerIteration;
    TDuration SleepQuantum;
    TDuration MessageRearrangeTimeout;

    explicit TNLBusServerConfig(int port = -1)
        : Port(port)
    {
        Register("port", Port);
        Register("max_nl_calls_per_iteration", MaxNLCallsPerIteration).Default(10);
        Register("sleep_quantum", SleepQuantum).Default(TDuration::MilliSeconds(10));
        Register("message_rearrange_timeout", MessageRearrangeTimeout).Default(TDuration::MilliSeconds(100));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBusServer::TPtr CreateNLBusServer(const TNLBusServerConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
