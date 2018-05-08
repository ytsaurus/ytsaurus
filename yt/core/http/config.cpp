#include "config.h"

namespace NYT {
namespace NHttp {

////////////////////////////////////////////////////////////////////////////////

TServerConfig::TServerConfig()
{
    RegisterParameter("port", Port)
        .Default(80);

    RegisterParameter("max_simultaneous_connections", MaxSimultaneousConnections)
        .Default(50000);

    RegisterParameter("read_buffer_size", ReadBufferSize)
        .Default(128_KB);

    RegisterParameter("write_buffer_size", WriteBufferSize)
        .Default(128_KB);

    RegisterParameter("bind_retry_count", BindRetryCount)
        .Default(5);

    RegisterParameter("bind_retry_backoff", BindRetryBackoff)
        .Default(TDuration::Seconds(1));
}

////////////////////////////////////////////////////////////////////////////////

TClientConfig::TClientConfig()
{
    RegisterParameter("read_buffer_size", ReadBufferSize)
        .Default(128_KB);

    RegisterParameter("write_buffer_size", WriteBufferSize)
        .Default(128_KB);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHttp
} // namespace NYT
