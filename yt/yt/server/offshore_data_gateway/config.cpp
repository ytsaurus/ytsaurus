#include "config.h"

namespace NYT::NOffshoreDataGateway {

////////////////////////////////////////////////////////////////////////////////

void TOffshoreDataGatewayBootstrapConfig::Register(TRegistrar registrar)
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
        .Default("//sys/offshore_data_gateways/@config");
}

void TOffshoreDataGatewayProgramConfig::Register(TRegistrar /*registrar*/)
{ }

void TOffshoreDataGatewayDynamicConfig::Register(TRegistrar registrar)
{
    registrar.Parameter("storage_thread_count", &TThis::StorageThreadCount)
        .GreaterThan(0)
        .Default(2);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NOffshoreDataGateway
