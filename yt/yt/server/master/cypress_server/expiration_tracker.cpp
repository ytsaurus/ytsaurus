#include "expiration_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"

#include <yt/yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/yt/server/lib/hydra/mutation.h>

#include <yt/yt/server/master/cell_master/bootstrap.h>
#include <yt/yt/server/master/cell_master/hydra_facade.h>
#include <yt/yt/server/master/cell_master/config.h>
#include <yt/yt/server/master/cell_master/config_manager.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <yt/yt/library/profiling/producer.h>


namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NHydra;
using namespace NObjectClient;
using namespace NProfiling;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TExpirationTracker::TExpirationTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{
    ExpirationTrackerProfiler()
        .WithDefaultDisabled()
        .WithGlobal()
        .WithTag("cell_tag", ToString(Bootstrap_->GetMulticellManager()->GetCellTag()))
        .AddProducer("", BufferedProducer_);
}

void TExpirationTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    THashSet<TCypressNode*> expiredNodes;
    for (auto& shard : Shards_) {
        for (auto* expiredNode : shard.ExpiredNodes) {
            expiredNodes.insert(expiredNode);
        }
        shard.ExpiredNodes = THashSet<TCypressNode*>();
    }

    YT_LOG_INFO("Started registering node expiration (Count: %v)",
        expiredNodes.size());

    for (auto* trunkNode : expiredNodes) {
        if (trunkNode->GetExpirationTimeIterator()) {
            UnregisterNodeExpirationTime(trunkNode);
        }

        if (trunkNode->GetExpirationTimeoutIterator()) {
            UnregisterNodeExpirationTimeout(trunkNode);
        }

        if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
            RegisterNodeExpirationTime(trunkNode, *expirationTime);
        }

        if (trunkNode->TryGetExpirationTimeout() && !IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode);
        }
    }
    YT_LOG_INFO("Finished registering node expiration");

    BufferedProducer_->SetEnabled(true);

    YT_VERIFY(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::CypressNodeExpirationTracker),
        BIND(&TExpirationTracker::RunCheckIteration, MakeWeak(this)));
    CheckExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TExpirationTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    BufferedProducer_->SetEnabled(false);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    for (auto& shard : Shards_) {
        shard.ExpirationMap.clear();
        shard.ExpiredNodes.clear();
    }
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNode* trunkNode, std::optional<TInstant> oldExpirationTime)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    auto expirationTime = trunkNode->TryGetExpirationTime();
    if (trunkNode->GetExpirationTimeIterator()) {
        if (expirationTime == oldExpirationTime) {
            // Fast path.
            return;
        }

        UnregisterNodeExpirationTime(trunkNode);
    }

    // NB: Sometimes the node can be created with expiration time, but be held past it in the same transaction
    // that created it. In this case the node might end up unregistered from the expiration maps.
    // See TExpirationTracker::OnNodeRemovalFailed().
    if (expirationTime) {
        YT_LOG_DEBUG("Node expiration time set (NodeId: %v, ExpirationTime: %v)",
            trunkNode->GetId(),
            *expirationTime);
        RegisterNodeExpirationTime(trunkNode, *expirationTime);
    } else if (oldExpirationTime) {
        YT_LOG_DEBUG("Node expiration time reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeExpirationTimeoutUpdated(TCypressNode* trunkNode, std::optional<TDuration> oldExpirationTimeout)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    auto expirationTimeout = trunkNode->TryGetExpirationTimeout();
    if (trunkNode->GetExpirationTimeoutIterator()) {
        if (expirationTimeout == oldExpirationTimeout) {
            // Fast path.
            return;
        }

        UnregisterNodeExpirationTimeout(trunkNode);
    }

    // See TExpirationTracker::OnNodeExpirationTimeUpdated().
    if (expirationTimeout) {
        YT_LOG_DEBUG("Node expiration timeout set (NodeId: %v, ExpirationTimeout: %v)",
            trunkNode->GetId(),
            *expirationTimeout);
        if (!IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode);
        }
    } else if (oldExpirationTimeout) {
        YT_LOG_DEBUG("Node expiration timeout reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeTouched(TCypressNode* trunkNode)
{
    YT_VERIFY(HasMutationContext());

    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    auto* shard = GetShard(trunkNode);
    auto guard = Guard(shard->Lock);

    if (trunkNode->GetExpirationTimeoutIterator()) {
        UnregisterNodeExpirationTimeout(trunkNode);
    }

    if (trunkNode->TryGetExpirationTimeout() && !IsNodeLocked(trunkNode)) {
        auto expirationTimeout = trunkNode->GetExpirationTimeout();
        auto expirationTime = trunkNode->GetTouchTime() + expirationTimeout;
        YT_LOG_TRACE("Node is scheduled to expire by timeout (NodeId: %v, ExpirationTimeout: %v, AnticipatedExpirationTime: %v)",
            trunkNode->GetId(),
            expirationTimeout,
            expirationTime);
        RegisterNodeExpirationTimeout(trunkNode);
    }
}

void TExpirationTracker::OnNodeDestroyed(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeIterator()) {
        UnregisterNodeExpirationTime(trunkNode);
    }

    if (trunkNode->GetExpirationTimeoutIterator()) {
        UnregisterNodeExpirationTimeout(trunkNode);
    }

    // NB: Typically missing.
    auto* shard = GetShard(trunkNode);
    shard->ExpiredNodes.erase(trunkNode);
}

void TExpirationTracker::OnNodeRemovalFailed(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    auto* shard = GetShard(trunkNode);

    // This usually means that either:
    //   1. Node was created under a transaction, and was held past
    //      it's expiration time by the same tx.
    //   2. Node is already a part of a subtree, that was detached and
    //      scheduled for destruction.
    // In both cases it's fine to just remove it from the expiration map
    // to avoid checking it multiple times. It will either be registered again
    // when merging branches, or be removed on its own.
    if (!trunkNode->GetReachable()) {
        // NB: Typically missing at followers.
        shard->ExpiredNodes.erase(trunkNode);
        return;
    }

    if (trunkNode->TryGetExpirationTime() && !trunkNode->GetExpirationTimeIterator()) {
        // NB: Typically missing at followers.
        shard->ExpiredNodes.erase(trunkNode);

        auto* mutationContext = GetCurrentMutationContext();
        RegisterNodeExpirationTime(trunkNode, mutationContext->GetTimestamp() + GetDynamicConfig()->ExpirationBackoffTime);
    }

    if (trunkNode->TryGetExpirationTimeout() && !trunkNode->GetExpirationTimeoutIterator()) {
        // NB: Typically missing at followers.
        shard->ExpiredNodes.erase(trunkNode);

        // This happens when removing a map node fails due to a lock on a nested node.
        if (!IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode);
        } // Else expiration will be rescheduled on lock release.
    }

    // Prolong expiration in case of removal failure. This is debatable but seems safer.
    // NB: time(out) iterators differ on the leader and followers. Avoid relying
    // on them when updating persistent state.
    if (trunkNode->TryGetExpirationTimeout() && !IsNodeLocked(trunkNode)) {
        auto* mutationContext = GetCurrentMutationContext();
        trunkNode->SetTouchTime(mutationContext->GetTimestamp());
    }
}

TExpirationTracker::TShard* TExpirationTracker::GetShard(TCypressNode* node)
{
    Bootstrap_->VerifyPersistentStateRead();

    auto nodeId = node->GetId();
    auto shardIndex = GetShardIndex<ShardCount>(nodeId);
    return &Shards_[shardIndex];
}

void TExpirationTracker::RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeIterator());

    auto* shard = GetShard(trunkNode);
    auto& expiredNodes = shard->ExpiredNodes;
    if (expiredNodes.find(trunkNode) == expiredNodes.end()) {
        auto it = shard->ExpirationMap.emplace(expirationTime, trunkNode);
        trunkNode->SetExpirationTimeIterator(it);
    }
}

void TExpirationTracker::RegisterNodeExpirationTimeout(TCypressNode* trunkNode)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeoutIterator());

    auto* shard = GetShard(trunkNode);
    auto& expiredNodes = shard->ExpiredNodes;
    if (expiredNodes.find(trunkNode) == expiredNodes.end()) {
        auto touchTime = trunkNode->GetTouchTime();
        YT_VERIFY(touchTime);
        auto expirationTimeout = trunkNode->GetExpirationTimeout();
        auto it = shard->ExpirationMap.emplace(touchTime + expirationTimeout, trunkNode);
        trunkNode->SetExpirationTimeoutIterator(it);
    }
}

void TExpirationTracker::UnregisterNodeExpirationTime(TCypressNode* trunkNode)
{
    auto* shard = GetShard(trunkNode);
    shard->ExpirationMap.erase(*trunkNode->GetExpirationTimeIterator());
    trunkNode->SetExpirationTimeIterator(std::nullopt);
}

void TExpirationTracker::UnregisterNodeExpirationTimeout(TCypressNode* trunkNode)
{
    auto* shard = GetShard(trunkNode);
    shard->ExpirationMap.erase(*trunkNode->GetExpirationTimeoutIterator());
    trunkNode->SetExpirationTimeoutIterator(std::nullopt);
}

bool TExpirationTracker::IsNodeLocked(TCypressNode* trunkNode) const
{
    return !trunkNode->LockingState().AcquiredLocks.empty();
}

void TExpirationTracker::RunCheckIteration()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    auto checkTime = NProfiling::GetInstant();
    CollectAndRemoveExpiredNodes(checkTime);
    UpdateProfiling(checkTime);
}

void TExpirationTracker::UpdateProfiling(TInstant checkTime)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    auto checkLag = TDuration::Zero();
    for (auto& shard : Shards_) {
        const auto& expirationMap = shard.ExpirationMap;
        if (expirationMap.empty()) {
            continue;
        }

        auto it = expirationMap.begin();
        auto expirationTime = it->first;
        checkLag = std::max(checkLag, checkTime - expirationTime);
    }

    TSensorBuffer buffer;
    buffer.AddGauge("/expiration_time_lag", checkLag.MilliSeconds());
    BufferedProducer_->Update(std::move(buffer));
}

void TExpirationTracker::CollectAndRemoveExpiredNodes(TInstant checkTime)
{
    NProto::TReqRemoveExpiredNodes request;

    auto canRemoveMoreExpiredNodes = [&] {
        return request.node_ids_size() < GetDynamicConfig()->MaxExpiredNodesRemovalsPerCommit;
    };

    for (auto& shard : Shards_) {
        const auto& expirationMap = shard.ExpirationMap;
        while (!expirationMap.empty() && canRemoveMoreExpiredNodes())
        {
            auto it = expirationMap.begin();
            auto [expirationTime, trunkNode] = *it;

            if (expirationTime > checkTime) {
                break;
            }

            // See comment for TShard::ExpirationMap.
            if (shard.ExpiredNodes.insert(trunkNode).second) {
                ToProto(request.add_node_ids(), trunkNode->GetId());
            }

            if (trunkNode->GetExpirationTimeIterator() && *trunkNode->GetExpirationTimeIterator() == it) {
                UnregisterNodeExpirationTime(trunkNode);
            } else if (trunkNode->GetExpirationTimeoutIterator() && *trunkNode->GetExpirationTimeoutIterator() == it) {
                UnregisterNodeExpirationTimeout(trunkNode);
            } else {
                YT_ABORT();
            }
        }

        if (!canRemoveMoreExpiredNodes()) {
            break;
        }
    }

    if (request.node_ids_size() == 0) {
        return;
    }

    YT_LOG_DEBUG("Starting removal commit for expired nodes (Count: %v)",
        request.node_ids_size());

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    YT_UNUSED_FUTURE(CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger()));
}

bool TExpirationTracker::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

const TDynamicCypressManagerConfigPtr& TExpirationTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->CypressManager;
}

void TExpirationTracker::OnDynamicConfigChanged(TDynamicClusterConfigPtr /*oldConfig*/)
{
    if (CheckExecutor_) {
        CheckExecutor_->SetPeriod(GetDynamicConfig()->ExpirationCheckPeriod);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
