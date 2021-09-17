#include "access_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"
#include "node.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/hydra/hydra_context.h>

#include <yt/yt/ytlib/cypress_client/cypress_service_proxy.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectServer;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TAccessTracker::TAccessTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TAccessTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_VERIFY(!FlushExecutor_);
    FlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::Periodic),
        BIND(&TAccessTracker::OnFlush, MakeWeak(this)));
    FlushExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TAccessTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    FlushExecutor_.Reset();

    Reset();
}

void TAccessTracker::SetModified(
    TCypressNode* node,
    EModificationType modificationType)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto* hydraContext = GetCurrentHydraContext();
    node->SetModificationTime(hydraContext->GetTimestamp());

    auto currentRevision = GetCurrentHydraContext()->GetVersion().ToRevision();

    switch (modificationType) {
        case EModificationType::Attributes:
            node->SetAttributeRevision(currentRevision);
            break;
        case EModificationType::Content:
            node->SetContentRevision(currentRevision);
            break;
        default:
            YT_ABORT();
    }
}

void TAccessTracker::SetAccessed(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(FlushExecutor_);
    YT_VERIFY(trunkNode->IsTrunk());
    YT_VERIFY(trunkNode->IsAlive());

    int index = trunkNode->GetAccessStatisticsUpdateIndex();
    if (index < 0) {
        index = UpdateAccessStatisticsRequest_.updates_size();
        trunkNode->SetAccessStatisticsUpdateIndex(index);
        NodesWithAccessStatisticsUpdate_.push_back(trunkNode);

        auto* update = UpdateAccessStatisticsRequest_.add_updates();
        ToProto(update->mutable_node_id(), trunkNode->GetId());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralRefObject(trunkNode);
    }

    auto now = NProfiling::GetInstant();
    auto* update = UpdateAccessStatisticsRequest_.mutable_updates(index);
    update->set_access_time(ToProto<i64>(now));
    update->set_access_counter_delta(update->access_counter_delta() + 1);
}

void TAccessTracker::SetTouched(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_VERIFY(FlushExecutor_);
    YT_VERIFY(trunkNode->IsTrunk());
    YT_VERIFY(trunkNode->IsAlive());

    auto index = trunkNode->GetTouchNodesIndex();
    if (index < 0) {
        index = TouchNodesRequest_.node_ids_size();
        trunkNode->SetTouchNodesIndex(index);
        TouchedNodes_.push_back(trunkNode);

        ToProto(TouchNodesRequest_.add_node_ids(), trunkNode->GetId());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->EphemeralRefObject(trunkNode);
    }
}

void TAccessTracker::Reset()
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* node : NodesWithAccessStatisticsUpdate_) {
        if (node->IsAlive()) {
            node->SetAccessStatisticsUpdateIndex(-1);
        }
        objectManager->EphemeralUnrefObject(node);
    }

    for (auto* node : TouchedNodes_) {
        if (node->IsAlive()) {
            node->SetTouchNodesIndex(-1);
        }

        objectManager->EphemeralUnrefObject(node);
    }

    UpdateAccessStatisticsRequest_.Clear();
    TouchNodesRequest_.Clear();
    NodesWithAccessStatisticsUpdate_.clear();
    TouchedNodes_.clear();
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActive()) {
        return;
    }

    std::vector<TFuture<void>> asyncResults;

    if (!NodesWithAccessStatisticsUpdate_.empty()) {
        YT_LOG_DEBUG("Starting access statistics commit (NodeCount: %v)",
            UpdateAccessStatisticsRequest_.updates_size());

        auto mutation = CreateMutation(hydraManager, UpdateAccessStatisticsRequest_);
        mutation->SetAllowLeaderForwarding(true);
        auto asyncResult = mutation->CommitAndLog(Logger).AsVoid();
        asyncResults.push_back(std::move(asyncResult));
    }

    if (!TouchedNodes_.empty()) {
        YT_LOG_DEBUG("Sending node touch request to leader (NodeCount: %v)",
            TouchedNodes_.size());

        const auto& cellDirectory = Bootstrap_->GetCellDirectory();
        const auto& multicellManager = Bootstrap_->GetMulticellManager();
        auto leaderChannel = cellDirectory->GetChannel(multicellManager->GetCellId(), EPeerKind::Leader);

        TCypressServiceProxy leaderProxy(std::move(leaderChannel));
        auto leaderRequest = leaderProxy.TouchNodes();

        TouchNodesRequest_.Swap(leaderRequest.Get());

        auto asyncResult = leaderRequest->Invoke().AsVoid();
        // TODO(shakurov): fire & forget?
        asyncResults.push_back(std::move(asyncResult));
    }

    Reset();

    if (!asyncResults.empty()) {
        Y_UNUSED(WaitFor(AllSet(std::move(asyncResults))));
    }
}

const TDynamicCypressManagerConfigPtr& TAccessTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->CypressManager;
}

void TAccessTracker::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    FlushExecutor_->SetPeriod(GetDynamicConfig()->StatisticsFlushPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
