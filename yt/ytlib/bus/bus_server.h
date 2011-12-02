#pragma once

#include "common.h"
#include "bus.h"

#include "../misc/config.h"
#include "../ytree/ytree_fwd.h"

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct IBusServer
    : public virtual TRefCountedBase
{
    typedef TIntrusivePtr<IBusServer> TPtr;

    virtual void Start(IMessageHandler* handler) = 0;
    virtual void Stop() = 0;
    virtual void GetMonitoringInfo(NYTree::IYsonConsumer* consumer) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// TODO: place into a separate file (nl_bus_server.h/cpp?)

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

        SetDefaults();
    }
};

IBusServer::TPtr CreateNLBusServer(const TNLBusServerConfig& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT
