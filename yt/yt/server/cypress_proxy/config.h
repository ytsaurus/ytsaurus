#pragma once

#include "public.h"

#include <yt/yt/server/lib/cypress_registrar/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/core/bus/tcp/config.h>

namespace NYT::NCypressProxy {

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyConfig
    : public TNativeServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    TCypressRegistrarConfigPtr CypressRegistrar;

    TString RootPath;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TCypressProxyConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyConfig)

////////////////////////////////////////////////////////////////////////////////

class TObjectServiceDynamicConfig
    : public NYTree::TYsonStruct
{
public:
    //! Size of the thread pool used for object service
    //! requests execution.
    int ThreadPoolSize;

    REGISTER_YSON_STRUCT(TObjectServiceDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TObjectServiceDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

class TCypressProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    TObjectServiceDynamicConfigPtr ObjectService;

    REGISTER_YSON_STRUCT(TCypressProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TCypressProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressProxy
