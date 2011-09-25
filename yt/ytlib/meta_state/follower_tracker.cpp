#include "follower_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

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

bool TFollowerTracker::IsFollowerActive(TPeerId followerId)
{
    return FollowerStates[followerId].State == EPeerState::Following;
}

bool TFollowerTracker::HasActiveQuorum()
{
    int activeFollowerCount = 0;
    for (int i = 0; i < CellManager->GetPeerCount(); ++i) {
        auto& followerState = FollowerStates[i];
        if (followerState.State == EPeerState::Following) {
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
    ChangeFollowerState(followerId, EPeerState::Stopped);
    FollowerStates[followerId].Lease = TLeaseManager::TLease();
}

void TFollowerTracker::ChangeFollowerState(int followerId, EPeerState state)
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

void TFollowerTracker::ProcessPing(TPeerId followerId, EPeerState state)
{
    ChangeFollowerState(followerId, state);

    auto& followerState = FollowerStates[followerId];
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
