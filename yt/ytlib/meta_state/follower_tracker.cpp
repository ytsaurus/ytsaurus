#include "stdafx.h"
#include "follower_tracker.h"
#include "common.h"
#include "config.h"

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerTracker::TFollowerTracker(
    TFollowerTrackerConfig *config,
    TCellManager* cellManager,
    IInvokerPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , EpochControlInvoker(epochControlInvoker)
    , FollowerStates(cellManager->GetPeerCount())
    , ActiveFollowerCount(0)
{
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);

    YASSERT(config);
    YASSERT(cellManager);
    YASSERT(epochControlInvoker);

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        ResetFollowerState(id);
    }
}

void TFollowerTracker::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (CellManager->GetPeerCount() == 1) {
        LOG_INFO("Active quorum established");
    }
}

void TFollowerTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Do nothing, Stop is just for symmetry.
}

bool TFollowerTracker::IsFollowerActive(TPeerId followerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return FollowerStates[followerId].Status == EPeerStatus::Following;
}

bool TFollowerTracker::HasActiveQuorum() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return ActiveFollowerCount + 1 >= CellManager->GetQuorum();
}

void TFollowerTracker::ResetFollowerState(int followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ChangeFollowerStatus(followerId, EPeerStatus::Stopped);
    FollowerStates[followerId].Lease = TLeaseManager::NullLease;
}

void TFollowerTracker::ChangeFollowerStatus(int followerId, EPeerStatus status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto& followerState = FollowerStates[followerId];
    if (followerState.Status != status) {
        LOG_INFO("Follower %d status changed from %s to %s",
            followerId,
            ~followerState.Status.ToString(),
            ~status.ToString());

        bool oldHasQuorum = HasActiveQuorum();
        if (followerState.Status == EPeerStatus::Following) {
            --ActiveFollowerCount;
        }
        followerState.Status = status;
        if (status == EPeerStatus::Following) {
            ++ActiveFollowerCount;
        }
        bool newHasQuorum = HasActiveQuorum();

        if (oldHasQuorum && !newHasQuorum) {
            LOG_INFO("Active quorum lost");
        } else if (!oldHasQuorum && newHasQuorum) {
            LOG_INFO("Active quorum established");
        }
    }
}

void TFollowerTracker::OnLeaseExpired(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ResetFollowerState(followerId);
}

void TFollowerTracker::ProcessPing(TPeerId followerId, EPeerStatus status)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ChangeFollowerStatus(followerId, status);

    auto& followerState = FollowerStates[followerId];
    if (followerState.Lease == TLeaseManager::NullLease) {
        followerState.Lease = TLeaseManager::CreateLease(
            Config->PingTimeout,
            BIND(&TFollowerTracker::OnLeaseExpired, MakeStrong(this), followerId)
            .Via(~EpochControlInvoker));
    } else {
        TLeaseManager::RenewLease(followerState.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
