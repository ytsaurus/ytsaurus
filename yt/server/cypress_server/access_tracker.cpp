#include "stdafx.h"
#include "access_tracker.h"
#include "node.h"
#include "config.h"
#include "cypress_manager.h"
#include "private.h"

#include <core/profiling/timing.h>

#include <server/cell_master/bootstrap.h>
#include <server/cell_master/hydra_facade.h>

#include <server/object_server/object_manager.h>

#include <server/transaction_server/transaction.h>

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
        Config_->StatisticsFlushPeriod,
        EPeriodicExecutorMode::Manual);
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
    auto cypressManager = Bootstrap_->GetCypressManager();
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

        auto objectManager = Bootstrap_->GetObjectManager();
        objectManager->WeakRefObject(trunkNode);
    }

    auto now = NProfiling::CpuInstantToInstant(NProfiling::GetCpuInstant());
    auto* update = UpdateAccessStatisticsRequest_.mutable_updates(index);
    update->set_access_time(now.MicroSeconds());
    update->set_access_counter_delta(update->access_counter_delta() + 1);
}

void TAccessTracker::Reset()
{
    auto objectManager = Bootstrap_->GetObjectManager();
    for (auto* node : NodesWithAccessStatisticsUpdate_) {
        node->SetAccessStatisticsUpdateIndex(-1);
        objectManager->WeakUnrefObject(node);
    }    

    UpdateAccessStatisticsRequest_.Clear();
    NodesWithAccessStatisticsUpdate_.clear();
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    auto hydraManager = Bootstrap_->GetHydraFacade()->GetHydraManager();
    if (NodesWithAccessStatisticsUpdate_.empty() ||
        !hydraManager->IsActiveLeader() && !hydraManager->IsActiveFollower())
    {
        FlushExecutor_->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting access statistics commit for %v nodes",
        UpdateAccessStatisticsRequest_.updates_size());

    auto invoker = Bootstrap_->GetHydraFacade()->GetEpochAutomatonInvoker();
    CreateMutation(hydraManager, UpdateAccessStatisticsRequest_)
        ->SetAllowLeaderForwarding(true)
        ->Commit()
        .Subscribe(BIND([=, this_ = MakeStrong(this)] (const TErrorOr<TMutationResponse>& error) {
            if (error.IsOK()) {
                FlushExecutor_->ScheduleOutOfBand();
            } else {
                LOG_WARNING(error, "Error committing access statistics update");
            }
            FlushExecutor_->ScheduleNext();
        }).Via(invoker));

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
