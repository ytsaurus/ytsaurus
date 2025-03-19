#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/re2/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/library/auth_server/public.h>

namespace NYT::NKafkaProxy {

////////////////////////////////////////////////////////////////////////////////

struct TStringTransformationConfig
    : public virtual NYTree::TYsonStruct
{
    //! If set, replaces all non-overlapping matches of this pattern with the replacement string.
    NRe2::TRe2Ptr MatchPattern;
    TString Replacement;

    REGISTER_YSON_STRUCT(TStringTransformationConfig);

    static void Register(TRegistrar registrar);
};

DECLARE_REFCOUNTED_STRUCT(TStringTransformationConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
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

    //! Configures a list of transformations to be applied to the contents of the Kafka topic name in each request.
    //! Transformations are applied consecutively in order they are listed.
    //! Each transformation will replace all non-overlapping matches of its match pattern
    //! with the replacement string. You can use \1-\9 for captured match groups in the
    //! replacement string, and \0 for the whole match.
    //! Regex must follow RE2 syntax, which is a subset of that accepted by PCRE, roughly
    //! speaking, and with various caveats.
    std::vector<TStringTransformationConfigPtr> TopicNameTransformations;

    REGISTER_YSON_STRUCT(TProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyProgramConfig
    : public TProxyBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TGroupCoordinatorConfig
    : public virtual NYTree::TYsonStruct
{
    //! How long members will be waited during join and sync stages.
    TDuration RebalanceTimeout;

    //! How often members should send a heartbeat.
    TDuration SessionTimeout;

    REGISTER_YSON_STRUCT(TGroupCoordinatorConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TGroupCoordinatorConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
    int PollerThreadCount;
    int AcceptorThreadCount;

    std::optional<std::string> LocalHostName;

    TGroupCoordinatorConfigPtr GroupCoordinator;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafkaProxy
