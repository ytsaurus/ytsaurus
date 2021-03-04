#include "medium_updater.h"

#include "config.h"
#include "private.h"
#include "master_connector.h"
#include "chunk_store.h"
#include "location.h"

#include <yt/server/node/cluster_node/bootstrap.h>
#include <yt/server/node/cluster_node/config.h>
#include <yt/server/node/cluster_node/dynamic_config_manager.h>

#include <yt/ytlib/api/native/client.h>
#include <yt/ytlib/api/native/connection.h>

#include <yt/ytlib/chunk_client/medium_directory.h>
#include <yt/ytlib/chunk_client/medium_directory_synchronizer.h>

#include <yt/core/concurrency/periodic_executor.h>

#include <yt/core/misc/guid.h>

namespace NYT::NDataNode {

using namespace NApi;
using namespace NConcurrency;
using namespace NLogging;
using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = DataNodeLogger;

////////////////////////////////////////////////////////////////////////////////

TMediumUpdater::TMediumUpdater(NClusterNode::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
    , ControlInvoker_(Bootstrap_->GetControlInvoker())
{

    const auto& dynamicConfigManager = Bootstrap_->GetDynamicConfigManager();
    dynamicConfigManager->SubscribeConfigChanged(BIND(&TMediumUpdater::OnDynamicConfigChanged, MakeWeak(this)));
    const auto& config = dynamicConfigManager->GetConfig()->DataNode->MediumUpdater;
    Enabled_ = config->Enabled;
    Executor_ = New<TPeriodicExecutor>(
        ControlInvoker_,
        BIND(&TMediumUpdater::UpdateMedia, MakeWeak(this)),
        config->Period);
}

void TMediumUpdater::OnDynamicConfigChanged(
    const NClusterNode::TClusterNodeDynamicConfigPtr& /* oldNodeConfig */,
    const NClusterNode::TClusterNodeDynamicConfigPtr& newNodeConfig)
{
    const auto& config = newNodeConfig->DataNode->MediumUpdater;
    Executor_->SetPeriod(config->Period);
    if (!Enabled_ && config->Enabled) {
        Executor_->Start();
    } else if (Enabled_ && !config->Enabled) {
        Executor_->Stop();
    }
    Enabled_ = config->Enabled;
}

void TMediumUpdater::Start()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    if (!Enabled_) {
        return;
    }

    Executor_->Start();

    WaitFor(Executor_->GetExecutedEvent())
        .ThrowOnError();
}

TFuture<void> TMediumUpdater::Stop()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);

    return Executor_->Stop();
}

void TMediumUpdater::UpdateMedia()
{
    VERIFY_INVOKER_AFFINITY(ControlInvoker_);
    YT_LOG_DEBUG("Starting media update");

    const auto& client = Bootstrap_->GetMasterClient();

    auto myRpcAddress = Bootstrap_->GetDefaultLocalAddressOrThrow();
    auto mediumConfigPath = Format("//sys/cluster_nodes/%s/@config/medium_overrides", myRpcAddress);
    YT_LOG_DEBUG("Loading node media config (MediumConfigPath: %Qv)", mediumConfigPath);

    auto mediumConfigOrError = WaitFor(client->GetNode(mediumConfigPath));

    if (mediumConfigOrError.IsOK()) {
        try {
            MediumOverrides_ = NYT::NYTree::ConvertTo<TMediumOverrideMap>(mediumConfigOrError.Value());
        } catch (const TErrorException& ex) {
            // TODO(s-v-m): Also populate alert to MasterConnector.
            YT_LOG_WARNING(ex, "Can not parse medium config; Skipping reconfiguration (MediumConfigPath: %Qv)", mediumConfigPath);
            return;
        }
    } else if (mediumConfigOrError.FindMatching(NYTree::EErrorCode::ResolveError)){
        YT_LOG_DEBUG("Medium config does not exist at master; Using empty configuration");
        MediumOverrides_.clear();
    } else {
        YT_LOG_ERROR("Cannot load medium config from master; Skipping reconfiguration");
        return;
    }

    auto& chunkStore = Bootstrap_->GetChunkStore();
    if (!chunkStore) {
        YT_LOG_DEBUG("No chunk store is configured, skipping locations update");
        return;
    }
    for (const auto& location : chunkStore->Locations()) {
        auto it = MediumOverrides_.find(location->GetUuid());
        auto newName = it != MediumOverrides_.end()
            ? it->second
            : location->GetConfig()->MediumName;
        if (newName != location->GetMediumName()) {
            YT_LOG_INFO("Changing location medium (Location: %Qv, OldMedium: %Qv, NewMedium: %Qv)",
                location->GetUuid(),
                location->GetMediumName(),
                newName);
            location->UpdateMediumName(newName);
        }
    }
}

std::optional<TString> TMediumUpdater::GetMediumOverride(TLocationUuid locationUuid) const
{
    auto it = MediumOverrides_.find(locationUuid);
    if (it != MediumOverrides_.end()) {
        return it->second;
    } else {
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NDataNode

