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
    ClearFollowerStates();
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

void TFollowerStateTracker::ClearFollowerState(TFollowerState& followerState)
{
    followerState.State = TMasterStateManager::EState::Stopped;
    followerState.Lease = TLeaseManager::TLease();
}

void TFollowerStateTracker::ClearFollowerStates()
{
    for (TMasterId i = 0; i < CellManager->GetMasterCount(); ++i) {
        ClearFollowerState(FollowerStates[i]);
    }
}

void TFollowerStateTracker::OnLeaseExpired(TMasterId followerId)
{
    LOG_DEBUG("Leader ping lease expired (FollowerId: %d)", followerId);
    ClearFollowerState(FollowerStates[followerId]);
}

void TFollowerStateTracker::ProcessPing(TMasterId followerId, TMasterStateManager::EState state)
{
    TFollowerState& followerState = FollowerStates[followerId];

    if (followerState.State != state) {
        LOG_INFO("Follower state changed (FollowerId: %d, OldState: %s, NewState: %s)",
            followerId,
            ~followerState.State.ToString(),
            ~state.ToString());
        followerState.State = state;
    }

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
