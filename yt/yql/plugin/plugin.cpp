#include "plugin.h"
#include "config.h"

#include <iostream>

namespace NYT::NYqlPlugin {

using namespace NYson;

NYTree::IMapNodePtr IYqlPlugin::GetOrchidNode() const
{
    return NYTree::GetEphemeralNodeFactory()->CreateMap();
}

////////////////////////////////////////////////////////////////////////////////

TYqlPluginOptions ConvertToOptions(
    TYqlPluginConfigPtr config,
    TYsonString singletonsConfigString,
    THolder<TLogBackend> logBackend,
    std::string maxSupportedYqlVersion,
    bool startDqManager) 
{
    return TYqlPluginOptions {
        .SingletonsConfig = singletonsConfigString,
        .GatewayConfig = ConvertToYsonString(config->GatewayConfig),
        .DqGatewayConfig = config->EnableDQ ? ConvertToYsonString(config->DQGatewayConfig) : TYsonString(),
        .YtflowGatewayConfig = ConvertToYsonString(config->YtflowGatewayConfig),
        .DqManagerConfig = config->EnableDQ ? ConvertToYsonString(config->DQManagerConfig) : TYsonString(),
        .FileStorageConfig = ConvertToYsonString(config->FileStorageConfig),
        .OperationAttributes = ConvertToYsonString(config->OperationAttributes),
        .Libraries = ConvertToYsonString(config->Libraries),
        .YTTokenPath = config->YTTokenPath,
        .UIOrigin = config->UIOrigin,
        .LogBackend = std::move(logBackend),
        .YqlPluginSharedLibrary = config->YqlPluginSharedLibrary,
        .MaxYqlLangVersion = maxSupportedYqlVersion,
        .StartDqManager = startDqManager,
    };
}

////////////////////////////////////////////////////////////////////////////////

Y_WEAK std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions /*options*/) noexcept
{
    std::cerr << "No YQL plugin implementation is available; link against either "
              << "yt/yql/plugin/native or yt/yql/plugin/dynamic" << std::endl;
    exit(1);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
