#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/auth_server/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

class TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
    //! Kafka proxy will listen on this port.
    int Port;

    bool AbortOnUnrecognizedOptions;

    //! Listener will try to bind a socket with
    //! provided number of retries and backoff.
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

    NAuth::TAuthenticationManagerConfigPtr Auth;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    TCypressRegistrarConfigPtr CypressRegistrar;

    TSlruCacheConfigPtr ClientCache;

    REGISTER_YSON_STRUCT(TProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyProgramConfig
    : public TProxyBootstrapConfig
    , public TServerProgramConfig
{
public:
    REGISTER_YSON_STRUCT(TProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    int PollerThreadCount;
    int AcceptorThreadCount;

    std::optional<TString> LocalHostName;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
