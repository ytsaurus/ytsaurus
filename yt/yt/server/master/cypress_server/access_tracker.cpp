#include "access_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"
#include "node.h"

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/server/master/object_server/object_manager.h>

#include <yt/yt/server/master/transaction_server/transaction.h>

#include <yt/yt/server/lib/hydra_common/hydra_context.h>

#include <yt/yt/ytlib/hive/cell_directory.h>

#include <yt/yt/core/profiling/timing.h>

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NCellMaster;
using namespace NSecurityServer;

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
        BIND(&TAccessTracker::OnFlush, MakeWeak(this)),
        GetDynamicConfig()->StatisticsFlushPeriod);
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

void TAccessTracker::SetAccessed(TCypressNode* trunkNode)
{
    Bootstrap_->VerifyPersistentStateRead();

    YT_VERIFY(FlushExecutor_);
    YT_VERIFY(IsObjectAlive(trunkNode));
    YT_VERIFY(trunkNode->IsTrunk());

    auto* shard = GetShard(trunkNode);
    auto guard = Guard(shard->Lock);

    auto index = trunkNode->GetAccessStatisticsUpdateIndex();
    if (index < 0) {
        index = shard->UpdateAccessStatisticsRequest.updates_size();
        trunkNode->SetAccessStatisticsUpdateIndex(index);
        shard->NodesWithAccessStatisticsUpdate.push_back(trunkNode->GetId());

        auto* update = shard->UpdateAccessStatisticsRequest.add_updates();
        ToProto(update->mutable_node_id(), trunkNode->GetId());
    }

    auto now = NProfiling::GetInstant();
    auto* update = shard->UpdateAccessStatisticsRequest.mutable_updates(index);
    update->set_access_time(ToProto<i64>(now));
    update->set_access_counter_delta(update->access_counter_delta() + 1);
}

void TAccessTracker::SetTouched(TCypressNode* trunkNode)
{
    Bootstrap_->VerifyPersistentStateRead();

    YT_VERIFY(FlushExecutor_);
    YT_VERIFY(IsObjectAlive(trunkNode));
    YT_VERIFY(trunkNode->IsTrunk());

    auto* shard = GetShard(trunkNode);
    auto guard = Guard(shard->Lock);

    auto index = trunkNode->GetTouchNodesIndex();
    if (index < 0) {
        index = shard->TouchNodesRequest.updates_size();
        trunkNode->SetTouchNodesIndex(index);
        shard->TouchedNodes.push_back(trunkNode->GetId());

        auto* update = shard->TouchNodesRequest.add_updates();
        ToProto(update->mutable_node_id(), trunkNode->GetId());
    }

    auto now = NProfiling::GetInstant();
    auto* update = shard->TouchNodesRequest.mutable_updates(index);
    update->set_touch_time(ToProto<i64>(now));
}

void TAccessTracker::Reset()
{
    const auto& objectManager = Bootstrap_->GetObjectManager();

    for (auto& shard : Shards_) {
        for (auto nodeId : shard.NodesWithAccessStatisticsUpdate) {
            auto* node = objectManager->FindObject(nodeId)->As<TCypressNode>();
            if (IsObjectAlive(node)) {
                node->SetAccessStatisticsUpdateIndex(-1);
            }
        }

        for (auto nodeId: shard.TouchedNodes) {
            auto* node = objectManager->FindObject(nodeId)->As<TCypressNode>();
            if (IsObjectAlive(node)) {
                node->SetTouchNodesIndex(-1);
            }
        }

        shard.UpdateAccessStatisticsRequest.Clear();
        shard.TouchNodesRequest.Clear();
        shard.NodesWithAccessStatisticsUpdate.clear();
        shard.TouchedNodes.clear();
    }
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActive()) {
        return;
    }

    std::vector<TFuture<void>> asyncResults;

    int accessStatisticsUpdateCount = 0;
    int touchedNodeCount = 0;
    for (const auto& shard : Shards_) {
        accessStatisticsUpdateCount += shard.NodesWithAccessStatisticsUpdate.size();
        touchedNodeCount += shard.TouchedNodes.size();
    }

    if (accessStatisticsUpdateCount > 0) {
        YT_LOG_DEBUG("Starting access statistics commit (NodeCount: %v)",
            accessStatisticsUpdateCount);

        NProto::TReqUpdateAccessStatistics request;
        for (const auto& shard : Shards_) {
            for (const auto& update : shard.UpdateAccessStatisticsRequest.updates()) {
                *request.add_updates() = update;
            }
        }

        auto mutation = CreateMutation(hydraManager, request);
        mutation->SetAllowLeaderForwarding(true);
        auto asyncResult = mutation->CommitAndLog(Logger).AsVoid();
        asyncResults.push_back(std::move(asyncResult));
    }

    if (touchedNodeCount > 0) {
        YT_LOG_DEBUG("Sending node touch commit (NodeCount: %v)",
            touchedNodeCount);

        NProto::TReqTouchNodes request;
        for (const auto& shard : Shards_) {
            for (const auto& update : shard.TouchNodesRequest.updates()) {
                *request.add_updates() = update;
            }
        }

        auto mutation = CreateMutation(hydraManager, request);
        mutation->SetAllowLeaderForwarding(true);
        auto asyncResult = mutation->CommitAndLog(Logger).AsVoid();
        asyncResults.push_back(std::move(asyncResult));
    }

    Reset();

    if (!asyncResults.empty()) {
        Y_UNUSED(WaitFor(AllSet(std::move(asyncResults))));
    }
}

TAccessTracker::TShard* TAccessTracker::GetShard(TCypressNode* node)
{
    Bootstrap_->VerifyPersistentStateRead();

    auto nodeId = node->GetId();
    auto shardIndex = GetShardIndex<ShardCount>(nodeId);
    return &Shards_[shardIndex];
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
