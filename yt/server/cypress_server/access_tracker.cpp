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
    : Config(config)
    , Bootstrap(bootstrap)
{ }

void TAccessTracker::StartFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    YCHECK(!FlushExecutor);
    FlushExecutor = New<TPeriodicExecutor>(
        Bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker(),
        BIND(&TAccessTracker::OnFlush, MakeWeak(this)),
        Config->StatisticsFlushPeriod,
        EPeriodicExecutorMode::Manual);
    FlushExecutor->Start();
}

void TAccessTracker::StopFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (FlushExecutor) {
        FlushExecutor->Stop();
        FlushExecutor.Reset();
    }

    Reset();
}

void TAccessTracker::OnModify(
    TCypressNodeBase* trunkNode,
    TTransaction* transaction)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(trunkNode->IsTrunk());
    YCHECK(trunkNode->IsAlive());

    // Failure here means that the node wasn't indeed locked,
    // which is strange given that we're about to mark it as modified.
    TVersionedNodeId versionedId(trunkNode->GetId(), GetObjectId(transaction));
    auto cypressManager = Bootstrap->GetCypressManager();
    auto* node = cypressManager->GetNode(versionedId);

    auto* mutationContext = Bootstrap
        ->GetHydraFacade()
        ->GetHydraManager()
        ->GetMutationContext();

    node->SetModificationTime(mutationContext->GetTimestamp());
    node->SetRevision(mutationContext->GetVersion().ToRevision());
}

void TAccessTracker::OnAccess(TCypressNodeBase* trunkNode)
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);
    YCHECK(trunkNode->IsTrunk());
    YCHECK(trunkNode->IsAlive());

    auto* update = trunkNode->GetAccessStatisticsUpdate();
    if (!update) {
        update = UpdateAccessStatisticsRequest.add_updates();
        ToProto(update->mutable_node_id(), trunkNode->GetId());

        trunkNode->SetAccessStatisticsUpdate(update);
        NodesWithAccessStatisticsUpdate.push_back(trunkNode);

        auto objectManager = Bootstrap->GetObjectManager();
        objectManager->WeakRefObject(trunkNode);
    }

    auto now = NProfiling::CpuInstantToInstant(NProfiling::GetCpuInstant());
    update->set_access_time(now.MicroSeconds());
    update->set_access_counter_delta(update->access_counter_delta() + 1);
}

void TAccessTracker::Reset()
{
    auto objectManager = Bootstrap->GetObjectManager();
    for (auto* node : NodesWithAccessStatisticsUpdate) {
        node->SetAccessStatisticsUpdate(nullptr);
        objectManager->WeakUnrefObject(node);
    }    

    UpdateAccessStatisticsRequest.Clear();
    NodesWithAccessStatisticsUpdate.clear();
}

void TAccessTracker::OnFlush()
{
    VERIFY_THREAD_AFFINITY(AutomatonThread);

    if (NodesWithAccessStatisticsUpdate.empty()) {
        FlushExecutor->ScheduleNext();
        return;
    }

    LOG_DEBUG("Starting access statistics commit for %d nodes",
        UpdateAccessStatisticsRequest.updates_size());

    auto this_ = MakeStrong(this);
    auto invoker = Bootstrap->GetHydraFacade()->GetEpochAutomatonInvoker();
    Bootstrap
        ->GetCypressManager()
        ->CreateUpdateAccessStatisticsMutation(UpdateAccessStatisticsRequest)
        ->Commit()
        .Subscribe(BIND([this, this_] (TErrorOr<TMutationResponse> error) {
            if (error.IsOK()) {
                FlushExecutor->ScheduleOutOfBand();
            }
            FlushExecutor->ScheduleNext();
        }).Via(invoker));

    Reset();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NCypressServer
} // namespace NYT
