#include "expiration_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"

#include <yt/server/cypress_server/cypress_manager.pb.h>

#include <yt/server/hydra/mutation.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/client/object_client/helpers.h>

namespace NYT {
namespace NCypressServer {

using namespace NConcurrency;
using namespace NHydra;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TExpirationTracker::TExpirationTracker(
    TCypressManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{ }

void TExpirationTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    LOG_INFO("Started registering node expiration (Count: %v)",
        ExpiredNodes_.size());
    for (auto* trunkNode : ExpiredNodes_) {
        Y_ASSERT(!trunkNode->GetExpirationIterator());
        auto expirationTime = trunkNode->GetExpirationTime();
        if (expirationTime) {
            RegisterNodeExpiration(trunkNode, *expirationTime);
        }
    }
    ExpiredNodes_.clear();
    LOG_INFO("Finished registering node expiration");

    YCHECK(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(NCellMaster::EAutomatonThreadQueue::ExpirationTracker),
        BIND(&TExpirationTracker::OnCheck, MakeWeak(this)),
        Config_->ExpirationCheckPeriod);
    CheckExecutor_->Start();

}

void TExpirationTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CheckExecutor_.Reset();
}

void TExpirationTracker::Clear()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    ExpirationMap_.clear();
    ExpiredNodes_.clear();
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->GetExpirationIterator()) {
        UnregisterNodeExpiration(trunkNode);
    }

    if (trunkNode->GetExpirationTime()) {
        auto expirationTime = *trunkNode->GetExpirationTime();
        LOG_DEBUG_UNLESS(IsRecovery(), "Node expiration time set (NodeId: %v, ExpirationTime: %v)",
            trunkNode->GetId(),
            expirationTime);
        RegisterNodeExpiration(trunkNode, expirationTime);
    } else {
        LOG_DEBUG_UNLESS(IsRecovery(), "Node expiration time reset (NodeId: %v)",
            trunkNode->GetId());
    }
}

void TExpirationTracker::OnNodeDestroyed(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->GetExpirationIterator()) {
        UnregisterNodeExpiration(trunkNode);
    }

    // NB: Typically missing.
    ExpiredNodes_.erase(trunkNode);
}

void TExpirationTracker::OnNodeRemovalFailed(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    Y_ASSERT(trunkNode->IsTrunk());

    if (trunkNode->GetExpirationIterator()) {
        return;
    }

    if (!trunkNode->GetExpirationTime()) {
        return;
    }

    // NB: Typically missing at followers.
    ExpiredNodes_.erase(trunkNode);

    auto* mutationContext = GetCurrentMutationContext();
    RegisterNodeExpiration(trunkNode, mutationContext->GetTimestamp() + Config_->ExpirationBackoffTime);
}

void TExpirationTracker::RegisterNodeExpiration(TCypressNodeBase* trunkNode, TInstant expirationTime)
{
    if (ExpiredNodes_.find(trunkNode) == ExpiredNodes_.end()) {
        auto it = ExpirationMap_.emplace(expirationTime, trunkNode);
        trunkNode->SetExpirationIterator(it);
    }
}

void TExpirationTracker::UnregisterNodeExpiration(TCypressNodeBase* trunkNode)
{
    ExpirationMap_.erase(*trunkNode->GetExpirationIterator());
    trunkNode->SetExpirationIterator(Null);
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
    while (!ExpirationMap_.empty() && request.node_ids_size() < Config_->MaxExpiredNodesRemovalsPerCommit) {
        auto it = ExpirationMap_.begin();
        const auto& pair = *it;
        auto expirationTime = pair.first;
        auto* trunkNode = pair.second;
        Y_ASSERT(*trunkNode->GetExpirationIterator() == it);

        if (expirationTime > now) {
            break;
        }

        ToProto(request.add_node_ids(), trunkNode->GetId());
        UnregisterNodeExpiration(trunkNode);
        YCHECK(ExpiredNodes_.insert(trunkNode).second);
    }

    if (request.node_ids_size() == 0) {
        return;
    }

    LOG_DEBUG("Starting removal commit for expired nodes (Count: %v)",
        request.node_ids_size());

    CreateMutation(hydraManager, request)
        ->CommitAndLog(Logger);
}

bool TExpirationTracker::IsRecovery()
{
    return Bootstrap_->GetHydraFacade()->GetHydraManager()->IsRecovery();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
