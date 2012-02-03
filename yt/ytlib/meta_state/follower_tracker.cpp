#include "stdafx.h"
#include "follower_tracker.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerTracker::TFollowerTracker(
    TConfig* config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , CellManager(cellManager)
    , EpochInvoker(New<TCancelableInvoker>(serviceInvoker))
{
    VERIFY_INVOKER_AFFINITY(serviceInvoker, ControlThread);

    YASSERT(cellManager);
    YASSERT(serviceInvoker);

    FollowerStates = yvector<TFollowerState>(cellManager->GetPeerCount());

    ResetFollowerStates();
}

void TFollowerTracker::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    EpochInvoker->Cancel();
}

bool TFollowerTracker::IsFollowerActive(TPeerId followerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return FollowerStates[followerId].Status == EPeerStatus::Following;
}

bool TFollowerTracker::HasActiveQuorum() const
{
    VERIFY_THREAD_AFFINITY_ANY();

    int activeFollowerCount = 0;
    for (int i = 0; i < CellManager->GetPeerCount(); ++i) {
        auto& followerState = FollowerStates[i];
        if (followerState.Status == EPeerStatus::Following) {
            ++activeFollowerCount;
        }
    }
    return activeFollowerCount + 1 >= CellManager->GetQuorum();
}

void TFollowerTracker::ResetFollowerStates()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        ResetFollowerState(id);
    }
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
        LOG_INFO("Follower state changed (FollowerId: %d, OldStatus: %s, NewStatus: %s)",
            followerId,
            ~followerState.Status.ToString(),
            ~status.ToString());
        followerState.Status = status;
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
            ~FromMethod(&TFollowerTracker::OnLeaseExpired, TPtr(this), followerId)
            ->Via(~EpochInvoker));
    } else {
        TLeaseManager::RenewLease(followerState.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
