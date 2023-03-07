#include "access_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"
#include "node.h"

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/server/master/object_server/object_manager.h>

#include <yt/core/profiling/timing.h>

#include <yt/server/master/transaction_server/transaction.h>

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectServer;

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
    OnDynamicConfigChanged();
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

    const auto* mutationContext = GetCurrentMutationContext();
    node->SetModificationTime(mutationContext->GetTimestamp());

    switch (modificationType) {
        case EModificationType::Attributes:
            node->SetAttributesRevision(mutationContext->GetVersion().ToRevision());
            break;
        case EModificationType::Content:
            node->SetContentRevision(mutationContext->GetVersion().ToRevision());
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

void TAccessTracker::Reset()
{
    const auto& objectManager = Bootstrap_->GetObjectManager();
    for (auto* node : NodesWithAccessStatisticsUpdate_) {
        if (node->IsAlive()) {
            node->SetAccessStatisticsUpdateIndex(-1);
        }
        objectManager->EphemeralUnrefObject(node);
    }

    UpdateAccessStatisticsRequest_.Clear();
    NodesWithAccessStatisticsUpdate_.clear();
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (NodesWithAccessStatisticsUpdate_.empty() || !hydraManager->IsActive()) {
        return;
    }

    YT_LOG_DEBUG("Starting access statistics commit (NodeCount: %v)",
        UpdateAccessStatisticsRequest_.updates_size());

    auto mutation = CreateMutation(hydraManager, UpdateAccessStatisticsRequest_);
    mutation->SetAllowLeaderForwarding(true);
    auto asyncResult = mutation->CommitAndLog(Logger);

    Reset();

    Y_UNUSED(WaitFor(asyncResult));
}

const TDynamicCypressManagerConfigPtr& TAccessTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->CypressManager;
}

void TAccessTracker::OnDynamicConfigChanged()
{
    FlushExecutor_->SetPeriod(GetDynamicConfig()->StatisticsFlushPeriod);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
