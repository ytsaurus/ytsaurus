#include "access_tracker.h"
#include "private.h"
#include "config.h"
#include "cypress_manager.h"
#include "node.h"

#include <yt/server/cell_master/bootstrap.h>
#include <yt/server/cell_master/hydra_facade.h>

#include <yt/server/object_server/object_manager.h>

#include <yt/core/profiling/timing.h>

#include <yt/server/transaction_server/transaction.h>

namespace NYT {
namespace NCypressServer {

using namespace NConcurrency;
using namespace NHydra;
using namespace NTransactionServer;
using namespace NObjectServer;

////////////////////////////////////////////////////////////////////////////////

static const auto& Logger = CypressServerLogger;

////////////////////////////////////////////////////////////////////////////////

TAccessTracker::TAccessTracker(
    TCypressManagerConfigPtr config,
    NCellMaster::TBootstrap* bootstrap)
    : Config_(config)
    , Bootstrap_(bootstrap)
{ }

void TAccessTracker::Start()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(!FlushExecutor_);
    FlushExecutor_ = New<TPeriodicExecutor>(
        Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TAccessTracker::OnFlush, MakeWeak(this)),
        Config_->StatisticsFlushPeriod);
    FlushExecutor_->Start();
}

void TAccessTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    FlushExecutor_.Reset();
    Reset();
}

void TAccessTracker::SetModified(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(trunkNode->IsTrunk());
    YCHECK(trunkNode->IsAlive());

    // Failure here means that the node wasn't indeed locked,
    // which is strange given that we're about to mark it as modified.
    TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
    const auto& cypressManager = Bootstrap_->GetCypressManager();
    auto* node = cypressManager->GetNode(versionedId);

    const auto* mutationContext = GetCurrentMutationContext();
    node->SetModificationTime(mutationContext->GetTimestamp());
    node->SetRevision(mutationContext->GetVersion().ToRevision());
}

void TAccessTracker::SetAccessed(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(FlushExecutor_);
    YCHECK(trunkNode->IsTrunk());
    YCHECK(trunkNode->IsAlive());

    int index = trunkNode->GetAccessStatisticsUpdateIndex();
    if (index < 0) {
        index = UpdateAccessStatisticsRequest_.updates_size();
        trunkNode->SetAccessStatisticsUpdateIndex(index);
        NodesWithAccessStatisticsUpdate_.push_back(trunkNode);

        auto* update = UpdateAccessStatisticsRequest_.add_updates();
        ToProto(update->mutable_node_id(), trunkNode->GetId());

        const auto& objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakRefObject(trunkNode);
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
        objectManager->WeakUnrefObject(node);
    }    

    UpdateAccessStatisticsRequest_.Clear();
    NodesWithAccessStatisticsUpdate_.clear();
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    const auto& hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (NodesWithAccessStatisticsUpdate_.empty() ||
        !hydraManager->IsActiveLeader() && !hydraManager->IsActiveFollower())
    {
        return;
    }

    LOG_DEBUG("Starting access statistics commit for %v nodes",
        UpdateAccessStatisticsRequest_.updates_size());

    auto mutation = CreateMutation(hydraManager, UpdateAccessStatisticsRequest_);
    mutation->SetAllowLeaderForwarding(true);
    auto asyncResult = mutation->CommitAndLog(Logger);

    Reset();

    Y_UNUSED(WaitFor(asyncResult));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
