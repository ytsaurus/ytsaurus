#pragma once

#include <yt/core/ytree/yson_serializable.h>

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public NYTree::TYsonSerializable
{
public:
    int Port;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;

    int ReadBufferSize;
    int WriteBufferSize;

    TServerConfig() {
        RegisterParameter("port", Port)
            .Default(80);

        RegisterParameter("max_simultaneous_connections", MaxSimultaneousConnections)
            .Default(50000);

        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default(128_KB);

        RegisterParameter("write_buffer_size", WriteBufferSize)
            .Default(128_KB);
    }
};

DEFINE_REFCOUNTED_TYPE(TServerConfig);

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public NYTree::TYsonSerializable
{
public:
    int ReadBufferSize;
    int WriteBufferSize;

    TClientConfig() {
        RegisterParameter("read_buffer_size", ReadBufferSize)
            .Default(128_KB);

        RegisterParameter("write_buffer_size", WriteBufferSize)
            .Default(128_KB);
    }
};

DEFINE_REFCOUNTED_TYPE(TClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
