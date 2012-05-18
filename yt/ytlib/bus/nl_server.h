#pragma once

#include "common.h"
#include "server.h"

#include <ytlib/misc/configurable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TNLBusServerConfig
    : public TConfigurable
{
    typedef TIntrusivePtr<TNLBusServerConfig> TPtr;

    int Port;
    int MaxNLCallsPerIteration;
    TDuration SleepQuantum;
    TDuration MessageRearrangeTimeout;
    TDuration SessionTimeout;

    explicit TNLBusServerConfig(int port = -1)
        : Port(port)
    {
        Register("port", Port);
        Register("max_nl_calls_per_iteration", MaxNLCallsPerIteration)
            .Default(10);
        Register("sleep_quantum", SleepQuantum)
            .Default(TDuration::MilliSeconds(10));
        Register("message_rearrange_timeout", MessageRearrangeTimeout)
            .Default(TDuration::MilliSeconds(1000));
        Register("session_timeout", SessionTimeout)
            .Default(TDuration::Seconds(60));
    }
};

////////////////////////////////////////////////////////////////////////////////

IBusServer::TPtr CreateNLBusServer(TNLBusServerConfig::TPtr config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
