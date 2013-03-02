#pragma once

#include "public.h"

#include <ytlib/ytree/yson_serializable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpBusConfig
    : public TYsonSerializable
{
    int Priority;
    bool EnableNoDelay;
    bool EnableQuickAck;

    TTcpBusConfig()
    {
        Register("priority", Priority)
            .InRange(0, 6)
            .Default(0);
        Register("enable_no_delay", EnableNoDelay)
            .Default(true);
        Register("enable_quick_ack", EnableQuickAck)
            .Default(true);
    }
};

struct TTcpBusServerConfig
    : public TTcpBusConfig
{
    int Port;

    explicit TTcpBusServerConfig(int port = -1)
        : Port(port)
    {
        Register("port", Port);
    }
};

struct TTcpBusClientConfig
    : public TTcpBusConfig
{
    Stroka Address;

    explicit TTcpBusClientConfig(const Stroka& address = "")
        : Address(address)
    {
        Register("address", Address)
            .NonEmpty();
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

