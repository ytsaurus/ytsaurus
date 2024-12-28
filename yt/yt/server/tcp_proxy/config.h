#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/net/config.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

class TRouterConfig
    : public NYTree::TYsonStruct
{
public:
    NNet::TDialerConfigPtr Dialer;

    int MaxListenerBacklogSize;

    REGISTER_YSON_STRUCT(TRouterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRouterConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    std::string Role;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    TCypressRegistrarConfigPtr CypressRegistrar;

    TRouterConfigPtr Router;

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

class TRouterDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    TDuration RoutingTableUpdatePeriod;

    REGISTER_YSON_STRUCT(TRouterDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRouterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    int PollerThreadCount;
    int AcceptorThreadCount;

    TRouterDynamicConfigPtr Router;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
