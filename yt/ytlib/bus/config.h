#pragma once

#include "public.h"

#include <ytlib/misc/configurable.h>
    
namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

struct TTcpBusServerConfig
    : public TConfigurable
{
    int Port;
    int Priority;

    explicit TTcpBusServerConfig(int port = -1)
        : Port(port)
    {
        Register("port", Port);
        Register("priority", Priority)
            .InRange(0, 6)
            .Default(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTcpBusClientConfig
    : public TConfigurable
{
    Stroka Address;
    int Priority;

    explicit TTcpBusClientConfig(const Stroka& address = "")
        : Address(address)
    {
        Register("address", Address)
            .NonEmpty();
        Register("priority", Priority)
            .InRange(0, 6)
            .Default(0);
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

