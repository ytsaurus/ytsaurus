#include "dynamic_config_manager.h"

#include "bootstrap.h"
#include "config.h"
#include "private.h"

#include <yt/server/node/data_node/master_connector.h>

#include <yt/ytlib/api/native/client.h>

namespace NYT::NCellNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NYson;

const TLogger& Logger = CellNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TDynamicConfigManager::TDynamicConfigManager(
    TDynamicConfigManagerConfigPtr config,
    TBootstrap* bootstrap)
    : Config_(std::move(config))
    , Bootstrap_(bootstrap)
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
    , Executor_(New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TDynamicConfigManager::DoFetchConfig, MakeWeak(this)),
        Config_->ConfigFetchPeriod))
{ }

void TDynamicConfigManager::Start()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (Config_->Enabled) {
        YT_LOG_INFO("Starting dynamic config manager (ConfigFetchPeriod: %v)",
            Config_->ConfigFetchPeriod);

        Bootstrap_->GetMasterConnector()->SubscribePopulateAlerts(
            BIND(&TDynamicConfigManager::PopulateAlerts, MakeWeak(this)));
        Executor_->Start();

        // Fetch config for the first time before further node initialization.
        // In case of failure node will become read-only until successful config fetch.
        WaitFor(Executor_->GetExecutedEvent())
            .ThrowOnError();
    }
}

TFuture<void> TDynamicConfigManager::Stop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    return Executor_->Stop();
}

void TDynamicConfigManager::PopulateAlerts(std::vector<TError>* errors)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (errors && LastError_) {
        errors->push_back(*LastError_);
    }
    if (errors && LastUnrecognizedOptionError_) {
        errors->push_back(*LastUnrecognizedOptionError_);
    }
}

NYTree::IYPathServicePtr TDynamicConfigManager::GetOrchidService()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    auto producer = BIND(&TDynamicConfigManager::DoBuildOrchid, MakeStrong(this));
    return IYPathService::FromProducer(producer);
}

bool TDynamicConfigManager::IsDynamicConfigLoaded() const
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    return ConfigLoaded_;
}

void TDynamicConfigManager::DoFetchConfig()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    YT_LOG_INFO("Fetching dynamic node config");
    bool configUpdated = false;
    try {
        configUpdated = TryFetchConfig();
    } catch (std::exception& ex) {
        YT_LOG_WARNING(TError(ex));
        LastError_ = ex;
        return;
    }

    if (configUpdated) {
        LastError_ = std::nullopt;
    }
}

bool TDynamicConfigManager::TryFetchConfig()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    NApi::TGetNodeOptions options;
    options.ReadFrom = EMasterChannelKind::Cache;

    const auto& client = Bootstrap_->GetMasterClient();
    auto configOrError = WaitFor(client->GetNode("//sys/cluster_nodes/@config", options));
    if (!configOrError.IsOK()) {
        THROW_ERROR_EXCEPTION(EErrorCode::FailedToFetchDynamicConfig, "Failed to fetch dynamic config from Cypress")
            << configOrError;
    }

    auto configNode = ConvertTo<IMapNodePtr>(configOrError.Value());
    auto nodeTagList = Bootstrap_->GetMasterConnector()->GetLocalDescriptor().GetTags();

    auto nodeTagListChanged = (nodeTagList != CurrentNodeTagList_);
    if (nodeTagListChanged) {
        YT_LOG_INFO("Node tag list has changed (OldNodeTagList: %v, NewNodeTagList: %v)",
            CurrentNodeTagList_,
            nodeTagList);
        CurrentNodeTagList_ = nodeTagList;
    }

    std::optional<int> suitableConfigIndex;
    auto configs = configNode->GetChildren();
    for (int configIndex = 0; configIndex < configs.size(); ++configIndex) {
        if (MakeBooleanFormula(configs[configIndex].first).IsSatisfiedBy(CurrentNodeTagList_)) {
            if (suitableConfigIndex) {
                THROW_ERROR_EXCEPTION(EErrorCode::DuplicateSuitableDynamicConfigs,
                    "Found duplicate suitable dynamic configs (FirstConfigFilter: %v, SecondConfigFilter: %v)",
                    configs[*suitableConfigIndex].first,
                    configs[configIndex].first);
            }

            YT_LOG_INFO("Found suitable dynamic config (DynamicConfigFilter: %v)",
                configs[configIndex].first);
            suitableConfigIndex = configIndex;
        }
    }

    INodePtr newConfigNode;
    if (suitableConfigIndex) {
        newConfigNode = configs[*suitableConfigIndex].second;
    } else {
        YT_LOG_INFO("No suitable config found; using empty config");
        newConfigNode = GetEphemeralNodeFactory()->CreateMap();
    }

    if (AreNodesEqual(newConfigNode, CurrentConfig_)) {
        return false;
    }

    YT_LOG_INFO("Node dynamic config has changed, reconfiguring");

    auto newConfig = New<TCellNodeDynamicConfig>();
    newConfig->SetUnrecognizedStrategy(EUnrecognizedStrategy::KeepRecursive);
    try {
        newConfig->Load(newConfigNode);
    } catch (std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::InvalidDynamicConfig, "Invalid dynamic node config")
            << ex;
    }

    auto unrecognizedOptions = newConfig->GetUnrecognizedRecursively();
    if (unrecognizedOptions && unrecognizedOptions->GetChildCount() > 0 && Config_->EnableUnrecognizedOptionsAlert) {
        auto error = TError(EErrorCode::UnrecognizedDynamicConfigOption,
            "Found unrecognized options in dynamic config (UnrecognizedConfigOptions: %v)",
            ConvertToYsonString(unrecognizedOptions, EYsonFormat::Text));
        YT_LOG_WARNING(error);
        LastUnrecognizedOptionError_ = error;
    } else {
        LastUnrecognizedOptionError_ = std::nullopt;
    }

    try {
        Bootstrap_->OnDynamicConfigChanged(newConfig);
    } catch (std::exception& ex) {
        THROW_ERROR_EXCEPTION(EErrorCode::FailedToApplyDynamicConfig, "Failed to apply new dynamic config")
            << ex;
    }

    LastConfigUpdateTime_ = TInstant::Now();
    ConfigLoaded_ = true;
    CurrentConfig_ = newConfigNode;

    return true;
}

void TDynamicConfigManager::DoBuildOrchid(IYsonConsumer* consumer)
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("config").Value(CurrentConfig_)
            .Item("last_config_update_time").Value(LastConfigUpdateTime_)
        .EndMap();
}

DEFINE_REFCOUNTED_TYPE(TDynamicConfigManager)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCellNode

