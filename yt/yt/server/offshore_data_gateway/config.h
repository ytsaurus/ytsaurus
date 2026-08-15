#pragma once

#include "private.h"

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/ytlib/api/native/public.h>

#include <yt/yt/library/discovery_client/public.h>

#include <yt/yt/client/ypath/public.h>
#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/library/dynamic_config/config.h>

#include <yt/yt/library/program/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

struct TOffshoreDataGatewayBootstrapConfig
    : public NServer::TNativeServerBootstrapConfig
{
    bool AbortOnUnrecognizedOptions;

    NYTree::IMapNodePtr CypressAnnotations;

    NDynamicConfig::TDynamicConfigManagerConfigPtr DynamicConfigManager;
    NYPath::TYPath DynamicConfigPath;

    REGISTER_YSON_STRUCT(TOffshoreDataGatewayBootstrapConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayBootstrapConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOffshoreDataGatewayProgramConfig
    : public TOffshoreDataGatewayBootstrapConfig
    , public TServerProgramConfig
{
    REGISTER_YSON_STRUCT(TOffshoreDataGatewayProgramConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayProgramConfig)

////////////////////////////////////////////////////////////////////////////////

struct TOffshoreDataGatewayDynamicConfig
    : public TSingletonsDynamicConfig
{
    //! The number of threads in Storage thread pool (used for extracting chunk meta, handling
    //! chunk slices, columnar statistic etc).
    int StorageThreadCount;

    REGISTER_YSON_STRUCT(TOffshoreDataGatewayDynamicConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TOffshoreDataGatewayDynamicConfig)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
