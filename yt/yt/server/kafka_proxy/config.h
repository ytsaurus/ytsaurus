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

    REGISTER_YSON_STRUCT(TKafkaProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TGroupCoordinatorConfig
    : public virtual NYTree::TYsonStruct
{
public:
    //! How long members will be waited during join and sync stages.
    TDuration RebalanceTimeout;

    //! How often members should send a heartbeat.
    TDuration SessionTimeout;

    REGISTER_YSON_STRUCT(TGroupCoordinatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGroupCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

class TKafkaProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    int PollerThreadCount;
    int AcceptorThreadCount;

    std::optional<TString> LocalHostName;

    TGroupCoordinatorConfigPtr GroupCoordinator;

    REGISTER_YSON_STRUCT(TKafkaProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TKafkaProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
