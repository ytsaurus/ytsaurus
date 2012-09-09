#include "stdafx.h"
#include "quorum_tracker.h"
#include "private.h"
#include "config.h"

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TQuorumTracker::TQuorumTracker(
    TCellManagerPtr cellManager,
    IInvokerPtr epochControlInvoker)
    : CellManager(cellManager)
    , EpochControlInvoker(epochControlInvoker)
    , ActiveQuorumPromise(NewPromise<void>())
{
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    YCHECK(cellManager);
    YCHECK(epochControlInvoker);

    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        Statuses.push_back(peerId == CellManager->GetSelfId() ? EPeerStatus::Leading : EPeerStatus::Stopped);
    }
    ActivePeerCount = 0;

    OnPeerActive(CellManager->GetSelfId());
}

bool TQuorumTracker::IsPeerActive(TPeerId peerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto status = Statuses[peerId];
    return status == EPeerStatus::Leading ||
           status == EPeerStatus::Following;
}

bool TQuorumTracker::HasActiveQuorum() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActivePeerCount >= CellManager->GetQuorum();
}

void TQuorumTracker::SetStatus(TPeerId followerId, EPeerStatus status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (status == EPeerStatus::Following && Statuses[followerId] != EPeerStatus::Following) {
        OnPeerActive(followerId);
    }
    Statuses[followerId] = status;
}

TFuture<void> TQuorumTracker::GetActiveQuorum()
{
    return ActiveQuorumPromise;
}

void TQuorumTracker::OnPeerActive(TPeerId peerId)
{
    ++ActivePeerCount;
    if (ActivePeerCount == CellManager->GetQuorum()) {
        ActiveQuorumPromise.Set();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
