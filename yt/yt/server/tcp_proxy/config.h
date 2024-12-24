#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/dynamic_config/public.h>

#include <yt/yt/library/server_program/config.h>

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

class TTcpProxyConfig
    : public TNativeServerConfig
    , public TServerProgramConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    std::string Role;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;

    TCypressRegistrarConfigPtr CypressRegistrar;

    TRouterConfigPtr Router;

    REGISTER_YSON_STRUCT(TTcpProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpProxyConfig)

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

class TTcpProxyDynamicConfig
    : public TSingletonsDynamicConfig
{
public:
    int PollerThreadCount;
    int AcceptorThreadCount;

    TRouterDynamicConfigPtr Router;

    REGISTER_YSON_STRUCT(TTcpProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TTcpProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTcpProxy
