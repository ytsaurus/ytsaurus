#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

#include <yt/yt/core/net/config.h>

namespace NYT::NTcpProxy {

////////////////////////////////////////////////////////////////////////////////

struct TRouterConfig
    : public NYTree::TYsonStruct
{
    NNet::TDialerConfigPtr Dialer;

    int MaxListenerBacklogSize;

    REGISTER_YSON_STRUCT(TRouterConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRouterConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
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

struct TProxyProgramConfig
    : public TProxyBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TRouterDynamicConfig
    : public NYTree::TYsonStruct
{
    TDuration RoutingTableUpdatePeriod;

    REGISTER_YSON_STRUCT(TRouterDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TRouterDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

struct TProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
    int PollerThreadCount;
    int AcceptorThreadCount;

    TRouterDynamicConfigPtr Router;

    REGISTER_YSON_STRUCT(TProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
