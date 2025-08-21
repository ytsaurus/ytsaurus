#pragma once

#include "private.h"

#include <yt/yql/plugin/plugin.h>
#include <yt/yql/plugin/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NYqlPlugin::NProcess {

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
    TYqlPluginConfigPtr PluginConfig;
    TString MaxSupportedYqlVersion;
    bool StartDqManager;

    REGISTER_YSON_STRUCT(TYqlPluginProcessInternalConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TYqlPluginProcessInternalConfig)

} // namespace NYT::NYqlPlugin::NProcess