#pragma once

#include "public.h"

#include <yt/yt/core/net/public.h>

#include <yt/yt/core/ytree/yson_serializable.h>

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
    int MaxBacklogSize;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    bool EnableKeepAlive;

    bool CancelFiberOnConnectionClose;

    //! This field is not accessible from config.
    bool IsHttps = false;

    //! Used for thread naming.
    //! CamelCase identifiers are preferred.
    //! This field is not accessible from config.
    TString ServerName = "Http";

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
