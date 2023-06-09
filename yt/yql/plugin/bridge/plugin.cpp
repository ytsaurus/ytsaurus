#include "plugin.h"

#include "interface.h"

#include <yt/yql/plugin/plugin.h>
#include <util/system/dynlib.h>

#include <vector>
#include <optional>

namespace NYT::NYqlPlugin {
namespace NBridge {

////////////////////////////////////////////////////////////////////////////////

class TDynamicYqlPlugin
{
public:
    TDynamicYqlPlugin(std::optional<TString> yqlPluginSharedLibrary)
    {
        const TString DefaultYqlPluginLibraryName = "./libyqlplugin.so";
        auto sharedLibraryPath = yqlPluginSharedLibrary.value_or(DefaultYqlPluginLibraryName);
        Library_.Open(sharedLibraryPath.data());
        #define XX(function) function = reinterpret_cast<TFunc ## function*>(Library_.Sym(#function));
        FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX);
        #undef XX
    }

protected:
    #define XX(function) TFunc ## function* function;
    FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)
    #undef XX

    TDynamicLibrary Library_;
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public TDynamicYqlPlugin
    , public IYqlPlugin
{
public:
    explicit TYqlPlugin(TYqlPluginOptions& options)
        : TDynamicYqlPlugin(options.YqlPluginSharedLibrary)
    {
        std::vector<TBridgeYqlPluginOptions::TBridgeAdditionalCluster> bridgeAdditionalClusters;
        for (const auto& [cluster, proxy]: options.AdditionalClusters) {
            bridgeAdditionalClusters.push_back({
               .Cluster = cluster.data(),
               .Proxy = proxy.data(),
           });
        }

        TBridgeYqlPluginOptions bridgeOptions {
            .MRJobBinary = options.MRJobBinary.data(),
            .UdfDirectory = options.UdfDirectory.data(),
            .AdditionalClusterCount = static_cast<int>(bridgeAdditionalClusters.size()),
            .AdditionalClusters = bridgeAdditionalClusters.data(),
            .YTToken = options.YTToken.data(),
            .LogBackend = &options.LogBackend,
        };

        BridgePlugin_ = BridgeCreateYqlPlugin(&bridgeOptions);
    }

    TQueryResult Run(TString queryText) noexcept override
    {
        auto* bridgeQueryResult = BridgeRun(BridgePlugin_, queryText.data());
        auto toString = [] (const char* str, size_t strLength) -> std::optional<TString> {
            if (!str) {
                return std::nullopt;
            }
            return TString(str, strLength);
        };
        TQueryResult queryResult = {
            .YsonResult = toString(bridgeQueryResult->YsonResult, bridgeQueryResult->YsonResultLength),
            .Plan = toString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Statistics = toString(bridgeQueryResult->Statistics, bridgeQueryResult->StatisticsLength),
            .TaskInfo = toString(bridgeQueryResult->TaskInfo, bridgeQueryResult->TaskInfoLength),
            .YsonError = toString(bridgeQueryResult->YsonError, bridgeQueryResult->YsonErrorLength),
        };
        BridgeFreeQueryResult(bridgeQueryResult);
        return queryResult;
    }

    ~TYqlPlugin() override
    {
        BridgeFreeYqlPlugin(BridgePlugin_);
    }

private:
    TBridgeYqlPlugin* BridgePlugin_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NBridge

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IYqlPlugin> CreateYqlPlugin(TYqlPluginOptions& options) noexcept
{
    return std::make_unique<NBridge::TYqlPlugin>(options);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin::NBridge
