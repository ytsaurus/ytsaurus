#include "expiration_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"

#include <yt/server/cypress_server/cypress_manager.pb.h>

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/ytlib/object_client/helpers.h>

namespace NYT {
namespace NCypressServer {

using namespace NConcurrency;

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

    YCHECK(!CheckExecutor_);
    CheckExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TExpirationTracker::OnCheck, MakeWeak(this)),
        Config_->ExpirationCheckPeriod);
    CheckExecutor_->Start();

    LOG_INFO("Started checking node expiration times");

    auto cypressManager = Bootstrap_->GetCypressManager();
    for (const auto& pair : cypressManager->Nodes()) {
        auto* node = pair.second;
        if (node->IsTrunk() && node->GetExpirationTime()) {
            auto it = ExpirationTimeToNode_.insert(std::make_pair(*node->GetExpirationTime(), node));
            node->SetExpirationIterator(it);
        }
    }

    LOG_INFO("Finished checking node expiration times");
}

void TExpirationTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    CheckExecutor_.Reset();
    ExpirationTimeToNode_.clear();
}

void TExpirationTracker::OnNodeExpirationTimeUpdated(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(trunkNode->IsTrunk());

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
    YASSERT(trunkNode->IsTrunk());

    if (trunkNode->GetExpirationIterator()) {
        UnregisterNodeExpiration(trunkNode);
    }
}

void TExpirationTracker::OnNodeRemovalFailed(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YASSERT(trunkNode->IsTrunk());

    if (!trunkNode->GetExpirationIterator() && trunkNode->GetExpirationTime()) {
        RegisterNodeExpiration(trunkNode, TInstant::Now() + Config_->ExpirationBackoffTime);
    }
}

void TExpirationTracker::RegisterNodeExpiration(TCypressNodeBase* trunkNode, TInstant expirationTime)
{
    auto it = ExpirationTimeToNode_.insert(std::make_pair(expirationTime, trunkNode));
    trunkNode->SetExpirationIterator(it);
}

void TExpirationTracker::UnregisterNodeExpiration(TCypressNodeBase* trunkNode)
{
    ExpirationTimeToNode_.erase(*trunkNode->GetExpirationIterator());
    trunkNode->SetExpirationIterator(Null);
}

void TExpirationTracker::OnCheck()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (!hydraManager->IsActiveLeader()) {
        return;
    }

    NProto::TReqRemoveExpiredNodes request;

    auto now = TInstant::Now();
    while (!ExpirationTimeToNode_.empty() && request.node_ids_size() < Config_->MaxExpiredNodesRemovalsPerCommit) {
        auto it = ExpirationTimeToNode_.begin();
        const auto& pair = *it;
        auto expirationTime = pair.first;
        auto* trunkNode = pair.second;
        YASSERT(*trunkNode->GetExpirationIterator() == it);

        if (expirationTime > now) {
            break;
        }

        ToProto(request.add_node_ids(), trunkNode->GetId());
        UnregisterNodeExpiration(trunkNode);
    }

    if (request.node_ids_size() == 0) {
        return;
    }

    LOG_DEBUG("Starting removal commit for %v expired nodes",
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
