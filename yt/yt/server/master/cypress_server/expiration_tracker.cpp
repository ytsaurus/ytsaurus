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

#include <yt/yt/library/profiling/producer.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NCypressServer {

using namespace NCellMaster;
using namespace NConcurrency;
using namespace NCypressClient;
using namespace NHydra;
using namespace NObjectClient;
using namespace NObjectServer;
using namespace NProfiling;
using namespace NSecurityServer;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = CypressServerLogger;

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
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    auto previouslyScheduledForRemovalNodes = std::exchange(ScheduledForRemovalNodes_, {});

    YT_LOG_INFO("Started registering node expiration (Count: %v)", previouslyScheduledForRemovalNodes.size());

    for (auto* trunkNode : previouslyScheduledForRemovalNodes) {
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
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    BufferedProducer_->SetEnabled(false);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

    ExpirationMap_.clear();
    ScheduledForRemovalNodes_.clear();
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNode* trunkNode, std::optional<TInstant> oldExpirationTime)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
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
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
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
    ScheduledForRemovalNodes_.erase(trunkNode);
}

void TExpirationTracker::OnNodeRemovalFailed(TCypressNode* trunkNode)
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

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
        ScheduledForRemovalNodes_.erase(trunkNode);
        return;
    }

    if (trunkNode->TryGetExpirationTime() && !trunkNode->GetExpirationTimeIterator()) {
        // NB: Typically missing at followers.
        ScheduledForRemovalNodes_.erase(trunkNode);

        // NB: Time(out) iterators differ on the leader and followers. Avoid relying
        // on them when updating persistent state. The use of |TInstant::Now()| is ok.
        auto now = TInstant::Now();
        auto newExpirationTime = trunkNode->GetExpirationTime() > now
            // Expiration time was updated concurrently.
            ? trunkNode->GetExpirationTime()
            : now + GetDynamicConfig()->ExpirationBackoffTime;
        RegisterNodeExpirationTime(trunkNode, newExpirationTime);
    }

    if (trunkNode->TryGetExpirationTimeout() && !trunkNode->GetExpirationTimeoutIterator()) {
        // NB: Typically missing at followers.
        ScheduledForRemovalNodes_.erase(trunkNode);

        if (!IsNodeLocked(trunkNode)) {
            // This is transient, |TInstant::Now()| is ok.
            auto now = TInstant::Now();
            auto touchTimeOverride = trunkNode->GetTouchTime() + trunkNode->GetExpirationTimeout() > now
                // Either expiration timeout or touch time was updated concurrently.
                ? std::nullopt
                : std::optional(now);
            RegisterNodeExpirationTimeout(trunkNode, touchTimeOverride);
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

void TExpirationTracker::RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeIterator());

    if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
        auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
        trunkNode->SetExpirationTimeIterator(it);
    }
}

void TExpirationTracker::RegisterNodeExpirationTimeout(
    TCypressNode* trunkNode,
    std::optional<TInstant> touchTimeOverride)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeoutIterator());

    if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
        auto touchTime = touchTimeOverride.value_or(trunkNode->GetTouchTime());
        YT_VERIFY(touchTime);
        auto expirationTimeout = trunkNode->GetExpirationTimeout();
        auto it = ExpirationMap_.emplace(touchTime + expirationTimeout, trunkNode);
        trunkNode->SetExpirationTimeoutIterator(it);
    }
}

void TExpirationTracker::UnregisterNodeExpirationTime(TCypressNode* trunkNode)
{
    ExpirationMap_.erase(*trunkNode->GetExpirationTimeIterator());
    trunkNode->SetExpirationTimeIterator(std::nullopt);
}

void TExpirationTracker::UnregisterNodeExpirationTimeout(TCypressNode* trunkNode)
{
    ExpirationMap_.erase(*trunkNode->GetExpirationTimeoutIterator());
    trunkNode->SetExpirationTimeoutIterator(std::nullopt);
}

bool TExpirationTracker::IsNodeLocked(TCypressNode* trunkNode) const
{
    return !trunkNode->LockingState().AcquiredLocks.empty();
}

void TExpirationTracker::RunCheckIteration()
{
    YT_ASSERT_THREAD_AFFINITY(AutomatonThread);

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
    if (!ExpirationMap_.empty()) {
        auto expirationTime = ExpirationMap_.begin()->first;
        checkLag = std::max(checkLag, checkTime - expirationTime);
    }

    TSensorBuffer buffer;
    buffer.AddGauge("/expiration_time_lag", checkLag.MilliSeconds());
    BufferedProducer_->Update(std::move(buffer));
}

void TExpirationTracker::CollectAndRemoveExpiredNodes(TInstant checkTime)
{
    std::vector<TCypressNode*> expiredTrunkNodes;

    auto maxExpiredNodesRemovalsPerCommit = GetDynamicConfig()->MaxExpiredNodesRemovalsPerCommit;
    std::optional<bool> isSequoia;

    while (!ExpirationMap_.empty() &&
        std::ssize(expiredTrunkNodes) < maxExpiredNodesRemovalsPerCommit)
    {
        auto it = ExpirationMap_.begin();
        auto [expirationTime, trunkNode] = *it;

        if (expirationTime > checkTime) {
            break;
        }

        // See comment for ExpirationMap.
        if (!ScheduledForRemovalNodes_.contains(trunkNode)) {
            if (!isSequoia.has_value()) {
                isSequoia = trunkNode->IsSequoia();
            }

            // Sequoia and Cypress nodes are not expected to be living at
            // the same cell, so this check is just a precaution.
            if (isSequoia != trunkNode->IsSequoia()) {
                break;
            }

            if (IsObjectAlive(trunkNode)) {
                InsertOrCrash(ScheduledForRemovalNodes_, trunkNode);
                expiredTrunkNodes.push_back(trunkNode);
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

    if (expiredTrunkNodes.empty()) {
        return;
    }

    YT_LOG_DEBUG("Starting removal of expired %v nodes (Count: %v)",
        *isSequoia ? "Sequoia" : "Cypress",
        std::ssize(expiredTrunkNodes));

    if (*isSequoia || GetDynamicConfig()->RemoveExpiredMasterNodesViaClient) {
        std::vector<TEphemeralObjectPtr<TCypressNode>> trunkNodes;
        trunkNodes.reserve(expiredTrunkNodes.size());
        for (auto* trunkNode : expiredTrunkNodes) {
            trunkNodes.emplace_back(trunkNode);
        }

        RemoveExpiredNodesViaClient(trunkNodes);
    } else {
        RemoveExpiredNodesViaMutation(expiredTrunkNodes);
    }
}

void TExpirationTracker::RemoveExpiredNodesViaClient(const std::vector<TEphemeralObjectPtr<TCypressNode>>& trunkNodes)
{
    auto proxy = CreateObjectServiceWriteProxy(Bootstrap_->GetRootClient());
    auto batchReq = proxy.ExecuteBatch();

    for (const auto& trunkNode : trunkNodes) {
        auto targetPath = FromObjectId(trunkNode->GetId()) + "&";
        auto req = TYPathProxy::Remove(targetPath);
        req->set_recursive(true);

        auto* prerequisitesExt = req->Header().MutableExtension(
            NObjectClient::NProto::TPrerequisitesExt::prerequisites_ext);
        auto* prerequisiteRevision = prerequisitesExt->add_revisions();
        prerequisiteRevision->set_path(targetPath);
        prerequisiteRevision->set_revision(trunkNode->GetRevision().Underlying());

        // TODO(danilalexeev): YT-24752. Support TExpirationExt in Sequoia.
        SetCausedByNodeExpiration(&req->Header());

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
        if (rspOrError.IsOK()) {
            continue;
        }

        if (IsObjectAlive(trunkNode)) {
            OnNodeRemovalFailed(trunkNode.Get());
        }
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
