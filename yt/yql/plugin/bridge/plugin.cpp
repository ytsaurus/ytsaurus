#include "plugin.h"

#include "interface.h"

#include <yt/yql/plugin/plugin.h>

#include <yt/yt/core/misc/error.h>

#include <library/cpp/yt/misc/cast.h>

#include <util/system/dynlib.h>

namespace NYT::NYqlPlugin {
namespace NBridge {

////////////////////////////////////////////////////////////////////////////////

namespace {

std::optional<TString> ToString(const char* str, size_t strLength)
{
    if (!str) {
        return std::nullopt;
    }
    return TString(str, strLength);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

// Each YQL plugin ABI change should be listed here. Either a compat should be added
// or MinSupportedYqlPluginAbiVersion should be promoted.
DEFINE_ENUM(EYqlPluginAbiVersion,
    ((Invalid)            (-1))
    ((TheBigBang)          (0))
    ((AbortQuery)          (1)) // gritukan: Added BridgeAbort; no breaking changes.
    ((ValidateExplain)     (2)) // aleksandr.gaev: Adjusted BridgeRun to accept settings length and execution mode.
    ((DqManager)           (3)) // mpereskokova: Added BridgeStartYqlPlugin; Adjusted TBridgeYqlPluginOptions to save DQ configs.
    ((TemporaryTokens)     (4)) // mpereskokova: Added GetUsedClusters step; Changed Run options.
    ((Credentials)         (5)) // a-romanov: 'credentials' parameter instead of 'token'.
    ((DynamicConfig)       (6)) // lucius: Added OnDynamicConfigChanged.
);

constexpr auto MinSupportedYqlPluginAbiVersion = EYqlPluginAbiVersion::DynamicConfig;
constexpr auto MaxSupportedYqlPluginAbiVersion = EYqlPluginAbiVersion::DynamicConfig;

////////////////////////////////////////////////////////////////////////////////

class TDynamicYqlPlugin
{
public:
    TDynamicYqlPlugin(std::optional<TString> yqlPluginSharedLibrary)
    {
        static const TString DefaultYqlPluginLibraryName = "./libyqlplugin.so";
        auto sharedLibraryPath = yqlPluginSharedLibrary.value_or(DefaultYqlPluginLibraryName);
        Library_.Open(sharedLibraryPath.data());
        #define XX(function) function = reinterpret_cast<TFunc ## function*>(Library_.Sym(#function));

        // Firstly we need to get ABI version of the plugin to make it possible to
        // add compats for other functions.
        XX(BridgeGetAbiVersion);
        GetYqlPluginAbiVersion();

        FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX);
        #undef XX
    }

protected:
    TDynamicLibrary Library_;

    EYqlPluginAbiVersion AbiVersion_ = EYqlPluginAbiVersion::Invalid;

    #define XX(function) TFunc ## function* function;
    FOR_EACH_BRIDGE_INTERFACE_FUNCTION(XX)
    #undef XX

    void GetYqlPluginAbiVersion()
    {
        auto version = TryCheckedEnumCast<EYqlPluginAbiVersion>(BridgeGetAbiVersion());
        if (!version) {
            THROW_ERROR_EXCEPTION(
                "YQL plugin ABI version %v is not supported",
                BridgeGetAbiVersion());
        }

        if (*version < MinSupportedYqlPluginAbiVersion ||
            *version > MaxSupportedYqlPluginAbiVersion)
        {
            THROW_ERROR_EXCEPTION(
                "YQL plugin ABI version %Qv is not supported",
                *version);
        }

        AbiVersion_ = *version;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TYqlPlugin
    : public TDynamicYqlPlugin
    , public IYqlPlugin
{
public:
    explicit TYqlPlugin(TYqlPluginOptions options)
        : TDynamicYqlPlugin(options.YqlPluginSharedLibrary)
    {
        TString singletonsConfig = options.SingletonsConfig ? options.SingletonsConfig.ToString() : "{}";
        TString dqGatewayConfig = options.DqGatewayConfig ? options.DqGatewayConfig.ToString() : "";
        TString dqManagerConfig = options.DqManagerConfig ? options.DqManagerConfig.ToString() : "";

        TBridgeYqlPluginOptions bridgeOptions {
            .SingletonsConfig = singletonsConfig.data(),
            .SingletonsConfigLength = static_cast<int>(singletonsConfig.size()),
            .GatewayConfig = options.GatewayConfig.AsStringBuf().data(),
            .GatewayConfigLength = options.GatewayConfig.AsStringBuf().size(),
            .DqGatewayConfig = dqGatewayConfig.data(),
            .DqGatewayConfigLength = dqGatewayConfig.size(),
            .DqManagerConfig = dqManagerConfig.data(),
            .DqManagerConfigLength = dqManagerConfig.size(),
            .FileStorageConfig = options.FileStorageConfig.AsStringBuf().data(),
            .FileStorageConfigLength = options.FileStorageConfig.AsStringBuf().size(),
            .OperationAttributes = options.OperationAttributes.AsStringBuf().data(),
            .OperationAttributesLength = options.OperationAttributes.AsStringBuf().size(),
            .YTTokenPath = options.YTTokenPath.data(),
            .UIOrigin = options.UIOrigin.data(),
            .LogBackend = &options.LogBackend,
            .Libraries = options.Libraries.AsStringBuf().data(),
            .LibrariesLength = options.Libraries.AsStringBuf().size(),
        };

        BridgePlugin_ = BridgeCreateYqlPlugin(&bridgeOptions);
    }

    TQueryResult Run(
        TQueryId queryId,
        TString user,
        NYson::TYsonString credentials,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files,
        int executeMode) noexcept override
    {
        auto settingsString = settings ? settings.ToString() : "{}";
        auto queryIdStr = ToString(queryId);
        const auto credentialsString = credentials ? credentials.ToString() : "{}";

        std::vector<TBridgeQueryFile> filesData;
        filesData.reserve(files.size());
        for (const auto& file : files) {
            filesData.push_back(TBridgeQueryFile{
                .Name = file.Name.data(),
                .NameLength = file.Name.size(),
                .Content = file.Content.data(),
                .ContentLength = file.Content.size(),
                .Type = file.Type,
            });
        }

        auto* bridgeQueryResult = BridgeRun(
            BridgePlugin_,
            queryIdStr.data(),
            user.data(),
            queryText.data(),
            settingsString.data(),
            settingsString.length(),
            filesData.data(),
            filesData.size(),
            executeMode,
            credentialsString.data(),
            credentialsString.length());
        TQueryResult queryResult{
            .YsonResult = ToString(bridgeQueryResult->YsonResult, bridgeQueryResult->YsonResultLength),
            .Plan = ToString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Statistics = ToString(bridgeQueryResult->Statistics, bridgeQueryResult->StatisticsLength),
            .Progress = ToString(bridgeQueryResult->Progress, bridgeQueryResult->ProgressLength),
            .TaskInfo = ToString(bridgeQueryResult->TaskInfo, bridgeQueryResult->TaskInfoLength),
            .Ast = ToString(bridgeQueryResult->Ast, bridgeQueryResult->AstLength),
            .YsonError = ToString(bridgeQueryResult->YsonError, bridgeQueryResult->YsonErrorLength),
        };
        BridgeFreeQueryResult(bridgeQueryResult);
        return queryResult;
    }

    TClustersResult GetUsedClusters(
        TQueryId queryId,
        TString queryText,
        NYson::TYsonString settings,
        std::vector<TQueryFile> files) noexcept override
    {
        auto settingsString = settings ? settings.ToString() : "{}";

        std::vector<TBridgeQueryFile> filesData;
        filesData.reserve(files.size());
        for (const auto& file : files) {
            filesData.push_back(TBridgeQueryFile{
                .Name = file.Name.data(),
                .NameLength = file.Name.size(),
                .Content = file.Content.data(),
                .ContentLength = file.Content.size(),
                .Type = file.Type,
            });
        }

        auto* bridgeClustersResult = BridgeGetUsedClusters(
            BridgePlugin_,
            ToString(queryId).data(),
            queryText.data(),
            settingsString.data(),
            settingsString.length(),
            filesData.data(),
            filesData.size());

        std::vector<TString> clusters(bridgeClustersResult->ClusterCount);
        for (ssize_t i = 0; i < bridgeClustersResult->ClusterCount; i++) {
            clusters[i] = TString(bridgeClustersResult->Clusters[i]);
        }
        TClustersResult queryResult{
            .Clusters = clusters,
            .YsonError = ToString(bridgeClustersResult->YsonError, bridgeClustersResult->YsonErrorLength),
        };
        BridgeFreeClustersResult(bridgeClustersResult);

        return queryResult;
    }

    void Start() override
    {
        BridgeStartYqlPlugin(BridgePlugin_);
    }

    TQueryResult GetProgress(TQueryId queryId) noexcept override
    {
        auto queryIdStr = ToString(queryId);
        auto* bridgeQueryResult = BridgeGetProgress(BridgePlugin_, queryIdStr.data());
        TQueryResult queryResult{
            .Plan = ToString(bridgeQueryResult->Plan, bridgeQueryResult->PlanLength),
            .Progress = ToString(bridgeQueryResult->Progress, bridgeQueryResult->ProgressLength),
            .Ast = ToString(bridgeQueryResult->Ast, bridgeQueryResult->AstLength),
        };
        BridgeFreeQueryResult(bridgeQueryResult);
        return queryResult;
    }

    TAbortResult Abort(TQueryId queryId) noexcept override
    {
        auto queryIdStr = ToString(queryId);
        auto* bridgeAbortResult = BridgeAbort(BridgePlugin_, queryIdStr.data());
        // COMPAT(gritukan): AbortQuery
        if (!bridgeAbortResult) {
            return {};
        }

        TAbortResult abortResult{
            .YsonError = ToString(bridgeAbortResult->YsonError, bridgeAbortResult->YsonErrorLength),
        };
        BridgeFreeAbortResult(bridgeAbortResult);
        return abortResult;
    }

    void OnDynamicConfigChanged(TYqlPluginDynamicConfig config) noexcept override
    {
        BridgeOnDynamicConfigChanged(BridgePlugin_, &config);
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

std::unique_ptr<IYqlPlugin> CreateBridgeYqlPlugin(TYqlPluginOptions options) noexcept
{
    return std::make_unique<NBridge::TYqlPlugin>(std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NYqlPlugin
