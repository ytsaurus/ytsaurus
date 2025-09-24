#pragma once

#include "private.h"

#include <yt/yt/server/lib/cypress_election/config.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/client/ypath/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/ytlib/discovery_client/public.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/program/config.h>

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyBootstrapConfig
    : public TNativeServerConfig
{
public:
    bool AbortOnUnrecognizedOptions;

    NYTree::IMapNodePtr CypressAnnotations;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    TString DynamicConfigPath;

    REGISTER_YSON_STRUCT(TOffshoreNodeProxyBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreNodeProxyBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyProgramConfig
    : public TOffshoreNodeProxyBootstrapConfig
{
public:
    REGISTER_YSON_STRUCT(TOffshoreNodeProxyProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreNodeProxyProgramConfig)

////////////////////////////////////////////////////////////////////////////////

class TOffshoreNodeProxyDynamicConfig
    : public TNativeSingletonsDynamicConfig
{
public:
    //! The number of threads in Storage thread pool (used for extracting chunk meta, handling
    //! chunk slices, columnar statistic etc).
    int StorageThreadCount;

    REGISTER_YSON_STRUCT(TOffshoreNodeProxyDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreNodeProxyDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
