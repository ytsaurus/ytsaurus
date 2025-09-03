#pragma once

#include "private.h"

#include <yt/yql/plugin/plugin.h>
#include <yt/yql/plugin/public.h>

#include <yt/yt/server/lib/misc/config.h>

#include <yt/yt/library/server_program/config.h>

namespace NYT::NYqlPlugin::NProcess {

////////////////////////////////////////////////////////////////////////////////

struct TProcessYqlPluginInternalConfig
    : public NServer::TNativeServerBootstrapConfig
    , public TServerProgramConfig
{
    int SlotIndex;
    TYqlPluginConfigPtr PluginConfig;
    TString MaxSupportedYqlVersion;
    TSingletonsConfigPtr SingletonsConfig;

    REGISTER_YSON_STRUCT(TProcessYqlPluginInternalConfig);

    static void Register(TRegistrar registrar);
};

DEFINE_REFCOUNTED_TYPE(TProcessYqlPluginInternalConfig)

} // namespace NYT::NYqlPlugin::NProcess