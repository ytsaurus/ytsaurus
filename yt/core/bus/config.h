#pragma once

#include "public.h"

#include <core/ytree/yson_serializable.h>

namespace NYT {
namespace NBus {

////////////////////////////////////////////////////////////////////////////////

class TTcpBusConfig
    : public NYTree::TYsonSerializable
{
public:
    int Priority;
    bool EnableNoDelay;
    bool EnableQuickAck;

    TTcpBusConfig()
    {
        RegisterParameter("priority", Priority)
            .InRange(0, 6)
            .Default(0);
        RegisterParameter("enable_no_delay", EnableNoDelay)
            .Default(true);
        RegisterParameter("enable_quick_ack", EnableQuickAck)
            .Default(true);
    }
};

DEFINE_REFCOUNTED_TYPE(TTcpBusConfig)

class TTcpBusServerConfig
    : public TTcpBusConfig
{
public:
    int Port;
    int MaxBacklogSize;

    explicit TTcpBusServerConfig(int port = -1)
        : Port(port)
    {
        RegisterParameter("port", Port);
        RegisterParameter("max_backlog_size", MaxBacklogSize)
            .Default(8192);
    }
};

DEFINE_REFCOUNTED_TYPE(TTcpBusServerConfig)

class TTcpBusClientConfig
    : public TTcpBusConfig
{
public:
    Stroka Address;

    explicit TTcpBusClientConfig(const Stroka& address = "")
        : Address(address)
    {
        RegisterParameter("address", Address)
            .NonEmpty();
    }
};

DEFINE_REFCOUNTED_TYPE(TTcpBusClientConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NBus
} // namespace NYT

