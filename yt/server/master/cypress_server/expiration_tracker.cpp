#include "expiration_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"

#include <yt/server/master/cypress_server/proto/cypress_manager.pb.h>

#include <yt/server/lib/hydra/mutation.h>

#include <yt/server/master/cell_master/bootstrap.h>
#include <yt/server/master/cell_master/hydra_facade.h>
#include <yt/server/master/cell_master/config.h>
#include <yt/server/master/cell_master/config_manager.h>

#include <yt/client/object_client/helpers.h>

namespace NYT::NCypressServer {

using namespace NConcurrency;
using namespace NHydra;

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
    for (auto* trunkNode : ExpiredNodes_) {
        YT_ASSERT(!trunkNode->GetExpirationIterator());
        if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
            RegisterNodeExpiration(trunkNode, *expirationTime);
        }
    }
    ExpiredNodes_.clear();
    YT_LOG_INFO("Finished registering node expiration");

    YT_VERIFY(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::CypressNodeExpirationTracker),
        BIND(&TExpirationTracker::OnCheck, MakeWeak(this)));
    CheckExecutor_->Start();

    const auto& configManager = Bootstrap_->GetConfigManager();
    configManager->SubscribeConfigChanged(DynamicConfigChangedCallback_);
    OnDynamicConfigChanged();
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

    if (trunkNode->GetExpirationIterator()) {
        UnregisterNodeExpiration(trunkNode);
    }

    if (auto expirationTime = trunkNode->TryGetExpirationTime()) {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node expiration time set (NodeId: %v, ExpirationTime: %v)",
            trunkNode->GetId(),
            *expirationTime);
        RegisterNodeExpiration(trunkNode, *expirationTime);
    } else {
        YT_LOG_DEBUG_UNLESS(IsRecovery(), "Node expiration time reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeDestroyed(TCypressNode* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YT_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->IsForeign()) {
        return;
    }

    if (trunkNode->GetExpirationIterator()) {
        UnregisterNodeExpiration(trunkNode);
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

    if (trunkNode->GetExpirationIterator()) {
        return;
    }

    if (!trunkNode->TryGetExpirationTime()) {
        return;
    }

    // NB: Typically missing at followers.
    ExpiredNodes_.erase(trunkNode);

    auto* mutationContext = GetCurrentMutationContext();
    RegisterNodeExpiration(trunkNode, mutationContext->GetTimestamp() + GetDynamicConfig()->ExpirationBackoffTime);
}

void TExpirationTracker::RegisterNodeExpiration(TCypressNode* trunkNode, TInstant expirationTime)
{
    if (ExpiredNodes_.find(trunkNode) == ExpiredNodes_.end()) {
        auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
        trunkNode->SetExpirationIterator(it);
    }
}

void TExpirationTracker::UnregisterNodeExpiration(TCypressNode* trunkNode)
{
    ExpirationMap_.erase(*trunkNode->GetExpirationIterator());
    trunkNode->SetExpirationIterator(std::nullopt);
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
        const auto& pair = *it;
        auto expirationTime = pair.first;
        auto* trunkNode = pair.second;
        YT_ASSERT(*trunkNode->GetExpirationIterator() == it);

        if (expirationTime > now) {
            break;
        }

        ToProto(request.add_node_ids(), trunkNode->GetId());
        UnregisterNodeExpiration(trunkNode);
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

const TDynamicCypressManagerConfigPtr& TExpirationTracker::GetDynamicConfig()
{
    const auto& configManager = Bootstrap_->GetConfigManager();
    return configManager->GetConfig()->CypressManager;
}

void TExpirationTracker::OnDynamicConfigChanged()
{
    if (CheckExecutor_) {
        CheckExecutor_->SetPeriod(GetDynamicConfig()->ExpirationCheckPeriod);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NCypressServer
