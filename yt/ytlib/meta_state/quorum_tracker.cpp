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
    TFollowerTrackerConfigPtr config,
    TCellManagerPtr cellManager,
    IInvokerPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , EpochControlInvoker(epochControlInvoker)
    , Peers(cellManager->GetPeerCount())
    , ActivePeerCount(0)
{
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(epochControlInvoker);
}

void TQuorumTracker::Start(TPeerId leaderId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        Peers[peerId].Status =
            peerId == CellManager->GetSelfId()
            ? EPeerStatus::Leading
            : EPeerStatus::Stopped;
    }

    UpdateActiveQuorum();
}

void TQuorumTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Do nothing, Stop is just for symmetry.
}

bool TQuorumTracker::IsFollowerActive(TPeerId followerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return Peers[followerId].Status == EPeerStatus::Following;
}

bool TQuorumTracker::HasActiveQuorum() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActivePeerCount >= CellManager->GetQuorum();
}

void TQuorumTracker::ResetFollowerState(int followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ChangeFollowerStatus(followerId, EPeerStatus::Stopped);
    Peers[followerId].Lease = TLeaseManager::NullLease;
}

void TQuorumTracker::ChangeFollowerStatus(int followerId, EPeerStatus status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& peer = Peers[followerId];
    if (peer.Status != status) {
        LOG_INFO("Follower %d status changed from %s to %s",
            followerId,
            ~peer.Status.ToString(),
            ~status.ToString());
        peer.Status = status;
        UpdateActiveQuorum();
    }
}

void TQuorumTracker::UpdateActiveQuorum()
{
    bool oldHasQuorum = HasActiveQuorum();
    ActivePeerCount = 0;
    FOREACH (const auto& peer, Peers) {
        if (peer.Status == EPeerStatus::Leading || peer.Status == EPeerStatus::Following) {
            ++ActivePeerCount;
        }
    }
    bool newHasQuorum = HasActiveQuorum();

    if (oldHasQuorum && !newHasQuorum) {
        LOG_INFO("Active quorum lost");
    } else if (!oldHasQuorum && newHasQuorum) {
        LOG_INFO("Active quorum established");
    }
}

void TQuorumTracker::OnLeaseExpired(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ResetFollowerState(followerId);
}

void TQuorumTracker::ProcessPing(TPeerId followerId, EPeerStatus status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ChangeFollowerStatus(followerId, status);

    auto& peer = Peers[followerId];
    if (peer.Lease == TLeaseManager::NullLease) {
        peer.Lease = TLeaseManager::CreateLease(
            Config->PingTimeout,
            BIND(&TQuorumTracker::OnLeaseExpired, MakeWeak(this), followerId)
            .Via(EpochControlInvoker));
    } else {
        TLeaseManager::RenewLease(peer.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
