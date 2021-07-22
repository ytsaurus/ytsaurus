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

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NCellMaster;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TExpirationTracker::TExpirationTracker(NCellMaster::TBootstrap* bootstrap)
    : Bootstrap_(bootstrap)
{ }

void TExpirationTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YT_LOG_INFO("Started registering node expiration (Count: %v)",
        ExpiredNodes_.size());
    THashSet<TCypressNode*> expiredNodes;
    expiredNodes.swap(ExpiredNodes_);
    auto now = TInstant::Now();
    for (auto* trunkNode : expiredNodes) {
        YT_ASSERT(!trunkNode->GetExpirationTimeIterator() && !trunkNode->GetExpirationTimeoutIterator());

        if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
            RegisterNodeExpirationTime(trunkNode, *expirationTime);
        }

        auto expirationTimeout = trunkNode->TryGetExpirationTimeout();
        if (expirationTimeout && !IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode, now + *expirationTimeout);
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
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->UnsubscribeConfigChanged(DynamicConfigChangedCallback_);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ExpirationMap_.clear();
    ExpiredNodes_.clear();
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeIterator()) {
        UnregisterNodeExpirationTime(trunkNode);
    }

    if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node expiration time set (NodeId: %v, ExpirationTime: %v)",
            trunkNode->GetId(),
            *expirationTime);
        RegisterNodeExpirationTime(trunkNode, *expirationTime);
    } else {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node expiration time reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeExpirationTimeoutUpdated(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeoutIterator()) {
        UnregisterNodeExpirationTimeout(trunkNode);
    }

    if (auto expirationTimeout = trunkNode->TryGetExpirationTimeout()) {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node expiration timeout set (NodeId: %v, ExpirationTimeout: %v)",
            trunkNode->GetId(),
            *expirationTimeout);
        if (!IsNodeLocked(trunkNode)) {
            RegisterNodeExpirationTimeout(trunkNode, TInstant::Now() + *expirationTimeout);
        }
    } else {
        YT_LOG_DEBUG_IF(IsMutationLoggingEnabled(), "Node expiration timeout reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeTouched(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());
    YT_VERIFY(trunkNode->TryGetExpirationTimeout());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationTimeoutIterator()) {
        UnregisterNodeExpirationTimeout(trunkNode);
    }

    if (!IsNodeLocked(trunkNode)) {
        auto expirationTimeout = trunkNode->GetExpirationTimeout();
        auto expirationTime = TInstant::Now() + expirationTimeout;
        YT_LOG_TRACE_IF(IsMutationLoggingEnabled(), "Node is scheduled to expire by timeout (NodeId: %v, ExpirationTimeout: %v, AnticipatedExpirationTime: %v)",
            trunkNode->GetId(),
            expirationTimeout,
            expirationTime);
        RegisterNodeExpirationTimeout(trunkNode, expirationTime);
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
    ExpiredNodes_.erase(trunkNode);
}

void TExpirationTracker::OnNodeRemovalFailed(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->TryGetExpirationTime() && !trunkNode->GetExpirationTimeIterator()) {
        // NB: Typically missing at followers.
        ExpiredNodes_.erase(trunkNode);

        auto* mutationContext = GetCurrentMutationContext();
        RegisterNodeExpirationTime(trunkNode, mutationContext->GetTimestamp() + GetDynamicConfig()->ExpirationBackoffTime);
    }

    if (trunkNode->TryGetExpirationTimeout() && !trunkNode->GetExpirationTimeoutIterator()) {
        // NB: Typically missing at followers.
        ExpiredNodes_.erase(trunkNode);

        if (!IsNodeLocked(trunkNode)) {
            auto* mutationContext = GetCurrentMutationContext();
            RegisterNodeExpirationTimeout(trunkNode, mutationContext->GetTimestamp() + trunkNode->GetExpirationTimeout());
        } // Else expiration will be rescheduled on lock release.
    }
}

void TExpirationTracker::RegisterNodeExpirationTime(TCypressNode* trunkNode, TInstant expirationTime)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeIterator());

    if (ExpiredNodes_.find(trunkNode) == ExpiredNodes_.end()) {
        auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
        trunkNode->SetExpirationTimeIterator(it);
    }
}

void TExpirationTracker::RegisterNodeExpirationTimeout(TCypressNode* trunkNode, TInstant expirationTime)
{
    YT_ASSERT(!trunkNode->GetExpirationTimeoutIterator());

    if (ExpiredNodes_.find(trunkNode) == ExpiredNodes_.end()) {
        auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
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

void TExpirationTracker::OnCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    NProto::TReqRemoveExpiredNodes request;

    auto now = TInstant::Now();
    while (!ExpirationMap_.empty() && request.node_ids_size() < GetDynamicConfig()->MaxExpiredNodesRemovalsPerCommit) {
        auto it = ExpirationMap_.begin();
        auto [expirationTime, trunkNode] = *it;

        if (expirationTime > now) {
            break;
        }

        ToProto(request.add_node_ids(), trunkNode->GetId());
        if (trunkNode->GetExpirationTimeIterator() && *trunkNode->GetExpirationTimeIterator() == it) {
            UnregisterNodeExpirationTime(trunkNode);
        } else if (trunkNode->GetExpirationTimeoutIterator() && *trunkNode->GetExpirationTimeoutIterator() == it) {
            UnregisterNodeExpirationTimeout(trunkNode);
        } else {
            YT_ABORT();
        }
        YT_VERIFY(ExpiredNodes_.insert(trunkNode).second);
    }

    if (request.node_ids_size() == 0) {
        return;
    }

    YT_LOG_DEBUG("Starting removal commit for expired nodes (Count: %v)",
        request.node_ids_size());

    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TExpirationTracker::IsRecovery() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

bool TExpirationTracker::IsMutationLoggingEnabled() const
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsMutationLoggingEnabled();
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
