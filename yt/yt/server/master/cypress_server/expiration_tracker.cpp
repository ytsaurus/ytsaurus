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

#include <yt/yt/server/master/security_server/access_log.h>

#include <yt/yt/client/object_client/helpers.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NCellMaster;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NSecurityServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

static constexpr auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TExpirationTracker::TExpirationTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TExpirationTracker::Start()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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

    YT_VERIFY(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::CypressNodeExpirationTracker),
        BIND(&TExpirationTracker::OnCheck, MakeWeak(this)));
    CheckExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
}

void TExpirationTracker::Stop()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    for (auto& shard : Shards_) {
        shard.ExpirationMap.clear();
        shard.ExpiredNodes.clear();
    }
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNode* trunkNode)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeIterator()) {
        UnregisterNodeExpirationTime(trunkNode);
    }

    if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
        YT_LOG_DEBUG("Node expiration time set (NodeId: %v, ExpirationTime: %v)",
            trunkNode->GetId(),
            *expirationTime);
        RegisterNodeExpirationTime(trunkNode, *expirationTime);
    } else {
        YT_LOG_DEBUG("Node expiration time reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeExpirationTimeoutUpdated(TCypressNode* trunkNode)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeoutIterator()) {
        UnregisterNodeExpirationTimeout(trunkNode);
    }

    if (auto expirationTimeout = trunkNode->TryGetExpirationTimeout()) {
        YT_LOG_DEBUG("Node expiration timeout set (NodeId: %v, ExpirationTimeout: %v)",
            trunkNode->GetId(),
            *expirationTimeout);
        if (!IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode);
        }
    } else {
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
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
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
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    auto* shard = GetShard(trunkNode);
    if (trunkNode->TryGetExpirationTime() && !trunkNode->GetExpirationTimeIterator()) {
        // NB: Typically missing at followers.
        shard->ExpiredNodes.erase(trunkNode);

        // NB: time(out) iterators differ on the leader and followers. Avoid relying
        // on them when updating persistent state. The use of |TInstant::Now()| is ok.
        RegisterNodeExpirationTime(trunkNode, TInstant::Now() + GetDynamicConfig()->ExpirationBackoffTime);
    }

    if (trunkNode->TryGetExpirationTimeout() && !trunkNode->GetExpirationTimeoutIterator()) {
        // NB: Typically missing at followers.
        shard->ExpiredNodes.erase(trunkNode);

        // This happens when removing a map node fails due to a lock on a nested node.
        if (!IsNodeLocked(trunkNode)) {
            // This is transient, |TInstant::Now()| is ok.
            RegisterNodeExpirationTimeout(trunkNode, TInstant::Now());
        } // Else expiration will be rescheduled on lock release.
    }

    // Prolong expiration in case of removal failure. This is debatable but seems safer.
    if (trunkNode->TryGetExpirationTimeout() && !IsNodeLocked(trunkNode)) {
        // When removing via client, failure is processed outside of a mutation.
        if (auto* mutationContext = TryGetCurrentMutationContext()) {
            trunkNode->SetTouchTime(mutationContext->GetTimestamp());
        }
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

void TExpirationTracker::RegisterNodeExpirationTimeout(
    TCypressNode* trunkNode,
    std::optional<TInstant> touchTimeOverride)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeoutIterator());

    auto* shard = GetShard(trunkNode);
    auto& expiredNodes = shard->ExpiredNodes;
    if (expiredNodes.find(trunkNode) == expiredNodes.end()) {
        auto touchTime = touchTimeOverride.value_or(trunkNode->GetTouchTime());
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

void TExpirationTracker::OnCheck()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    std::vector<TCypressNode*> trunkNodesToRemove;

    auto canRemoveMoreExpiredNodes = [&] {
        return std::ssize(trunkNodesToRemove) < GetDynamicConfig()->MaxExpiredNodesRemovalsPerCommit;
    };

    std::optional<bool> isSequoia;
    auto now = NProfiling::GetInstant();
    for (auto& shard : Shards_) {
        auto& expirationMap = shard.ExpirationMap;
        while (!expirationMap.empty() && canRemoveMoreExpiredNodes()) {
            auto it = expirationMap.begin();
            auto [expirationTime, trunkNode] = *it;

            if (expirationTime > now) {
                break;
            }

            // See comment for TShard::ExpirationMap.
            if (!shard.ExpiredNodes.contains(trunkNode)) {
                if (!isSequoia.has_value()) {
                    isSequoia = trunkNode->IsSequoia();
                }

                // Sequoia and Cypress nodes are not expected to be living at
                // the same cell, so this check is just a precaution.
                if (isSequoia != trunkNode->IsSequoia()) {
                    break;
                }

                if (IsObjectAlive(trunkNode)) {
                    InsertOrCrash(shard.ExpiredNodes, trunkNode);
                    trunkNodesToRemove.push_back(trunkNode);
                }
            }

            if (trunkNode->GetExpirationTimeIterator() && *trunkNode->GetExpirationTimeIterator() == it) {
                UnregisterNodeExpirationTime(trunkNode);
            } else if (trunkNode->GetExpirationTimeoutIterator() && *trunkNode->GetExpirationTimeoutIterator() == it) {
                UnregisterNodeExpirationTimeout(trunkNode);
            } else {
                YT_ABORT();
            }
        }
    }

    if (trunkNodesToRemove.empty()) {
        return;
    }

    YT_LOG_DEBUG("Starting removal of expired %v nodes (Count: %v)",
        *isSequoia ? "Sequoia" : "Cypress",
        std::ssize(trunkNodesToRemove));

    if (*isSequoia || GetDynamicConfig()->RemoveExpiredMasterNodesViaClient) {
        std::vector<TEphemeralObjectPtr<TCypressNode>> trunkNodes;
        trunkNodes.reserve(trunkNodesToRemove.size());
        for (auto* trunkNode : trunkNodesToRemove) {
            trunkNodes.emplace_back(trunkNode);
        }

        RemoveExpiredNodesViaClient(trunkNodes);
    } else {
        RemoveExpiredNodesViaMutation(trunkNodesToRemove);
    }
}

void TExpirationTracker::RemoveExpiredNodesViaClient(const std::vector<TEphemeralObjectPtr<TCypressNode>>& trunkNodes)
{
    auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetRootClient());
    auto batchReq = proxy.ExecuteBatch();

    using TTag = std::pair<TNodeId, TYPath>;

    const auto& cypressManager = Bootstrap_->GetCypressManager();
    for (const auto& trunkNode : trunkNodes) {
        auto targetPath = FromObjectId(trunkNode->GetId()) + "&";
        auto req = TYPathProxy::Remove(targetPath);

        auto* prerequisitesExt = req->Header().MutableExtension(
            NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        auto* prerequisiteRevision = prerequisitesExt->add_revisions();
        prerequisiteRevision->set_path(targetPath);
        prerequisiteRevision->set_revision(trunkNode->GetRevision().Underlying());

        // For access logging.
        req->Tag() = TTag(
            trunkNode->GetId(),
            cypressManager->GetNodePath(trunkNode.Get(), nullptr));

        batchReq->AddRequest(req);
    }

    auto batchRspOrError = WaitFor(batchReq->Invoke());
    if (!batchRspOrError.IsOK()) {
        for (const auto& trunkNode : trunkNodes) {
            if (IsObjectAlive(trunkNode)) {
                OnNodeRemovalFailed(trunkNode.Get());
            }
        }
        return;
    }

    auto rsps = batchRspOrError.Value()->GetResponses<TYPathProxy::TRspRemove>();
    YT_VERIFY(std::ssize(rsps) == std::ssize(trunkNodes));
    for (const auto& [rspOrError, trunkNode] : Zip(rsps, trunkNodes)) {
        if (!rspOrError.IsOK()) {
            if (IsObjectAlive(trunkNode)) {
                OnNodeRemovalFailed(trunkNode.Get());
            }
            continue;
        }

        auto [nodeId, path] = std::any_cast<TTag>(rspOrError.Value()->Tag());
        YT_LOG_ACCESS(
            nodeId,
            path,
            nullptr,
            "TtlRemove");
    }
}

void TExpirationTracker::RemoveExpiredNodesViaMutation(const std::vector<TCypressNode*>& trunkNodes)
{
    NProto::TReqRemoveExpiredNodes request;
    for (auto* trunkNode : trunkNodes) {
        ToProto(request.add_node_ids(), trunkNode->GetId());
    }

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
