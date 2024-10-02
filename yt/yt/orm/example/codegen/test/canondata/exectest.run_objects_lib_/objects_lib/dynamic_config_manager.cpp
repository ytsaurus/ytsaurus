// AUTOMATICALLY GENERATED. DO NOT EDIT!

#include "dynamic_config_manager.h"

#include <yt/yt/orm/server/master/yt_connector.h>

namespace NYT::NOrm::NExample::NServer::NLibrary {

////////////////////////////////////////////////////////////////////////////////

using namespace NYT::NDynamicConfig;

////////////////////////////////////////////////////////////////////////////////

class TDynamicConfigManager
    : public IDynamicConfigManager
{
public:
    TDynamicConfigManager(
        NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
        const TMasterConfigPtr& config)
        : IDynamicConfigManager(
            TDynamicConfigManagerOptions{
                .ConfigPath = config->DynamicConfigPath
                    ? config->DynamicConfigPath
                    : bootstrap->GetYTConnector()->GetMasterPath() + "/config",
                .Name = bootstrap->GetServiceName(),
                .ConfigIsTagged = false,
            },
            config->DynamicConfigManager,
            bootstrap->GetYTConnector()->GetControlClient(),
            bootstrap->GetControlInvoker(),
            bootstrap->GetInitialConfigNode())
    { }
};

////////////////////////////////////////////////////////////////////////////////

IDynamicConfigManagerPtr CreateDynamicConfigManager(
    NYT::NOrm::NServer::NMaster::IBootstrap* bootstrap,
    const TMasterConfigPtr& config)
{
    return NYT::New<TDynamicConfigManager>(bootstrap, config);
}

////////////////////////////////////////////////////////////////////////////////

} // NYT::NOrm::NExample::NServer::NLibrary
