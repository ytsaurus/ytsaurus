#pragma once

#include "private.h"
#include "public.h"

#include <yt/yt/core/logging/config.h>
#include <yt/yt/library/server_program/config.h>
#include <yt/yt/server/lib/misc/config.h>

#include <yt/yql/plugin/plugin.h>

namespace NYT::NYqlPlugin {
namespace NProcess {

struct TYqlProcessPluginConfig
    : public NYTree::TYsonStruct
{

    bool Enabled;
    int SlotsCount;
    TString SlotsRootPath;

    TDuration CheckProcessActiveDelay;

    NLogging::TLogManagerConfigPtr LogManagerTemplate;

    REGISTER_YSON_STRUCT(TYqlProcessPluginConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlProcessPluginConfig)

////////////////////////////////////////////////////////////////////////////////
struct TYqlProcessPluginOptions
    : public NYTree::TYsonStruct
{
    NYson::TYsonString SingletonsConfig;
    NYson::TYsonString GatewayConfig;
    std::optional<NYson::TYsonString> DqGatewayConfig;
    std::optional<NYson::TYsonString> DqManagerConfig;
    NYson::TYsonString FileStorageConfig;
    NYson::TYsonString OperationAttributes;
    NYson::TYsonString Libraries;

    TString YTTokenPath;

    std::optional<TString> YqlPluginSharedLibrary;

    REGISTER_YSON_STRUCT(TYqlProcessPluginOptions);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlProcessPluginOptions)

////////////////////////////////////////////////////////////////////////////////

struct TYqlPluginProcessInternalConfig
    : public NServer::TNativeServerBootstrapConfig
    , public TServerProgramConfig
{

    int SlotIndex;

    TYqlProcessPluginOptionsPtr PluginOptions;

    REGISTER_YSON_STRUCT(TYqlPluginProcessInternalConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginProcessInternalConfig)

} // namespace NProcess

} // namespace NYT::NYqlPlugin