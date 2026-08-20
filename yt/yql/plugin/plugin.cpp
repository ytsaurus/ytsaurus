#include "plugin.h"
#include "config.h"

#include <iostream>

namespace NYT::NYqlPlugin {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

IMapNodePtr IYqlPlugin::GetOrchidNode() const
{
    return GetEphemeralNodeFactory()->CreateMap();
}

////////////////////////////////////////////////////////////////////////////////

TYqlNativePluginOptions ConvertToNativePluginOptions(
    TYqlPluginConfigPtr config,
    TYqlPluginDynamicConfigPtr initialDynamicConfig,
    TYsonString singletonsConfigString,
    THolder<TLogBackend> logBackend,
    bool startDqManager)
{
    auto options = TYqlNativePluginOptions {
        .SingletonsConfig = singletonsConfigString,
        .GatewayConfig = ConvertToYsonString(config->GatewayConfig),
        .DqGatewayConfig = config->EnableDQ ? ConvertToYsonString(config->DQGatewayConfig) : TYsonString(),
        .YtflowGatewayConfig = ConvertToYsonString(config->YtflowGatewayConfig),
        .PqGatewayConfig = ConvertToYsonString(config->PQGatewayConfig),
        .SolomonGatewayConfig = ConvertToYsonString(config->SolomonGatewayConfig),
        .DqManagerConfig = config->EnableDQ ? ConvertToYsonString(config->DQManagerConfig) : TYsonString(),
        .FileStorageConfig = ConvertToYsonString(config->FileStorageConfig),
        .TvmConfig = ConvertToYsonString(config->TvmConfig),
        .YtAccessProviderConfig = ConvertToYsonString(config->YtAccessProviderConfig),
        .OperationAttributes = ConvertToYsonString(config->OperationAttributes),
        .Libraries = ConvertToYsonString(config->Libraries),
        .InitialDynamicConfig = ConvertToYsonString(initialDynamicConfig),
        .YTTokenPath = config->YTTokenPath,
        .StartDqManager = startDqManager,
    };

    options.LogBackend = std::move(logBackend);
    return options;
}

TYqlQTWorkerPluginOptions ConvertToQtWorkerPluginOptions(
    TYqlNativePluginOptions nativeOptions,
    THolder<TLogBackend> qtWorkerLogBackend,
    int qtWorkerInspectorPort,
    TString gatewaysConfigPath)
{
    TYqlQTWorkerPluginOptions options;
    static_cast<TYqlNativePluginOptions&>(options) = std::move(nativeOptions);
    options.QtWorkerInspectorPort = qtWorkerInspectorPort;
    options.QtWorkerLogBackend = std::move(qtWorkerLogBackend);
    options.GatewaysConfigPath = std::move(gatewaysConfigPath);
    return options;
}

////////////////////////////////////////////////////////////////////////////////

TString DetectFlavorFromSettings(const NYson::TYsonString& settings)
{
    auto settingsMap = NYTree::ConvertTo<NYTree::IMapNodePtr>(settings);
    return settingsMap->GetChildValueOrDefault<TString>("yql_flavor", TString{DefaultFlavor});
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
