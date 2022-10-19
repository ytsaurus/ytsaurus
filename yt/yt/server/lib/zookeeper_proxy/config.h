#pragma once

#include "public.h"

#include <yt/yt/core/ytree/yson_struct.h>

namespace NYT::NZookeeperProxy {

////////////////////////////////////////////////////////////////////////////////

struct TZookeeperServerConfig
    : public NYTree::TYsonStruct
{
    //! Zookeeper server will listen on this port.
    int Port;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;

    //! Maximum size of backlog for listener.
    int MaxBacklogSize;

    //! Listener will try to bind a socket with
    //! provided number of retries and backoff.
    int BindRetryCount;
    TDuration BindRetryBackoff;

    //! When reading a message, this timeout for
    //! packets is used.
    TDuration ReadIdleTimeout;

    //! When posting a message, this timeout for
    //! packets is used.
    TDuration WriteIdleTimeout;

    REGISTER_YSON_STRUCT(TZookeeperServerConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZookeeperServerConfig)

////////////////////////////////////////////////////////////////////////////////

struct TZookeeperProxyConfig
    : public NYTree::TYsonStruct
{
    TZookeeperServerConfigPtr Server;

    REGISTER_YSON_STRUCT(TZookeeperProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TZookeeperProxyConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NZookeeperProxy
