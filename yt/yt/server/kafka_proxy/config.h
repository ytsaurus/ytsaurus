#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h> // TODO

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

class TKafkaProxyConfig
    : public TNativeServerConfig
{
public:
    int Port;
    bool AbortOnUnrecognizedOptions;

    NAuth::TAuthenticationManagerConfigPtr Auth;

    int BindRetryCount;
    TDuration BindRetryBackoff;

    //! Limit for number of open TCP connections.
    int MaxSimultaneousConnections;

    //! Maximum size of backlog for listener.
    int MaxBacklogSize;

    //! When reading a message, this timeout for
    //! packets is used.
    TDuration ReadIdleTimeout;

    //! When posting a message, this timeout for
    //! packets is used.
    TDuration WriteIdleTimeout;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    TCypressRegistrarConfigPtr CypressRegistrar;

    TSlruCacheConfigPtr ClientCache;

    REGISTER_YSON_STRUCT(TKafkaProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TKafkaProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    int PollerThreadCount;
    int AcceptorThreadCount;

    REGISTER_YSON_STRUCT(TKafkaProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
