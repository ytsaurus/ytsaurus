#include "config.h"

namespace NYT::NOffshoreNodeProxy {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreNodeProxyBootstrapConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("abort_on_unrecognized_options", &TThis::AbortOnUnrecognizedOptions)
        .Default(false);
    registrar.Parameter("cypress_annotations", &TThis::CypressAnnotations)
        .Default(NYTree::BuildYsonNodeFluently()
            .BeginMap()
            .EndMap()
        ->AsMap());
    registrar.Parameter("dynamic_config_manager", &TThis::DynamicConfigManager)
        .DefaultNew();
    registrar.Parameter("dynamic_config_path", &TThis::DynamicConfigPath)
        .Default("//sys/offshore_node_proxies/config");
}

void TOffshoreNodeProxyProgramConfig::Register(TRegistrar /*registrar*/)
{ }

void TOffshoreNodeProxyDynamicConfig::Register(TRegistrar /*registrar*/)
{ }

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreNodeProxy
