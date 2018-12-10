#pragma once

#include "public.h"

#include <yt/core/net/public.h>

#include <yt/core/ytree/yson_serializable.h>

namespace NYT::NHttp {

////////////////////////////////////////////////////////////////////////////////

class THttpIOConfig
    : public NYTree::TYsonSerializable
{
public:
    int ReadBufferSize;

    TDuration ConnectionIdleTimeout;

    TDuration HeaderReadTimeout;
    TDuration BodyReadIdleTimeout;

    TDuration WriteIdleTimeout;

    THttpIOConfig();
};

DEFINE_REFCOUNTED_TYPE(THttpIOConfig);

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
    : public THttpIOConfig
{
public:
    //! If zero then the port is chosen automatically.
    int Port;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    bool EnableKeepAlive;

    TServerConfig();
};

DEFINE_REFCOUNTED_TYPE(TServerConfig);

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
    : public THttpIOConfig
{
public:
    NNet::TDialerConfigPtr Dialer;

    TClientConfig();
};

DEFINE_REFCOUNTED_TYPE(TClientConfig);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHttp
