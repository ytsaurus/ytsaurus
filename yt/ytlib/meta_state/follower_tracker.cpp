#include "follower_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerTracker::TFollowerTracker(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , CellManager(cellManager)
    , EpochInvoker(New<TCancelableInvoker>(serviceInvoker))
    , FollowerStates(cellManager->GetPeerCount())
    , LeaseManager(New<TLeaseManager>())
{
    ResetFollowerStates();
}

void TFollowerTracker::Stop()
{
    EpochInvoker->Cancel();
}

bool TFollowerTracker::HasActiveQuorum()
{
    int activeFollowerCount = 0;
    for (int i = 0; i < CellManager->GetPeerCount(); ++i) {
        TFollowerState& followerState = FollowerStates[i];
        if (followerState.State == TMetaStateManager::EState::Following) {
            ++activeFollowerCount;
        }
    }
    return activeFollowerCount + 1 >= CellManager->GetQuorum();
}

void TFollowerTracker::ResetFollowerStates()
{
    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        ResetFollowerState(id);
    }
}

void TFollowerTracker::ResetFollowerState(int followerId)
{
    ChangeFollowerState(followerId, TMetaStateManager::EState::Stopped);
    FollowerStates[followerId].Lease = TLeaseManager::TLease();
}

void TFollowerTracker::ChangeFollowerState(
    int followerId,
    TMetaStateManager::EState state)
{
    TFollowerState& followerState = FollowerStates[followerId];
    if (followerState.State != state) {
        LOG_INFO("Follower state changed (FollowerId: %d, OldState: %s, NewState: %s)",
            followerId,
            ~followerState.State.ToString(),
            ~state.ToString());
        followerState.State = state;
    }
}

void TFollowerTracker::OnLeaseExpired(TPeerId followerId)
{
    ResetFollowerState(followerId);
}

void TFollowerTracker::ProcessPing(TPeerId followerId, TMetaStateManager::EState state)
{
    ChangeFollowerState(followerId, state);

    TFollowerState& followerState = FollowerStates[followerId];
    if (followerState.Lease == TLeaseManager::TLease()) {
        followerState.Lease = LeaseManager->CreateLease(
            Config.PingTimeout,
            FromMethod(&TFollowerTracker::OnLeaseExpired, TPtr(this), followerId)
            ->Via(~EpochInvoker));
    } else {
        LeaseManager->RenewLease(followerState.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
