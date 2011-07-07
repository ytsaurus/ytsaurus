#include "follower_state_tracker.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MasterLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerStateTracker::TFollowerStateTracker(
    const TConfig& config,
    TCellManager::TPtr cellManager,
    IInvoker::TPtr epochInvoker,
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , CellManager(cellManager)
    , EpochInvoker(epochInvoker)
    , ServiceInvoker(serviceInvoker)
    , FollowerStates(cellManager->GetMasterCount())
    , LeaseManager(new TLeaseManager())
{
    ResetFollowerStates();
}

bool TFollowerStateTracker::HasActiveQuorum()
{
    int activeFollowerCount = 0;
    for (int i = 0; i < CellManager->GetMasterCount(); ++i) {
        TFollowerState& followerState = FollowerStates[i];
        if (followerState.State == TMasterStateManager::EState::Following) {
            ++activeFollowerCount;
        }
    }
    return activeFollowerCount + 1 >= CellManager->GetQuorum();
}

void TFollowerStateTracker::ResetFollowerStates()
{
    for (TMasterId id = 0; id < CellManager->GetMasterCount(); ++id) {
        ResetFollowerState(id);
    }
}

void TFollowerStateTracker::ResetFollowerState(int followerId)
{
    ChangeFollowerState(followerId, TMasterStateManager::EState::Stopped);
    FollowerStates[followerId].Lease = TLeaseManager::TLease();
}

void TFollowerStateTracker::ChangeFollowerState(
    int followerId,
    TMasterStateManager::EState state)
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

void TFollowerStateTracker::OnLeaseExpired(TMasterId followerId)
{
    ResetFollowerState(followerId);
}

void TFollowerStateTracker::ProcessPing(TMasterId followerId, TMasterStateManager::EState state)
{
    ChangeFollowerState(followerId, state);

    TFollowerState& followerState = FollowerStates[followerId];
    if (followerState.Lease == TLeaseManager::TLease()) {
        followerState.Lease = LeaseManager->CreateLease(
            Config.PingTimeout,
            FromMethod(&TFollowerStateTracker::OnLeaseExpired, TPtr(this), followerId)
            ->Via(~EpochInvoker)
            ->Via(ServiceInvoker));
    } else {
        LeaseManager->RenewLease(followerState.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
