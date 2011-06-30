#include "follower_state_tracker.h"

#include "../logging/log.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger Logger("FollowerStateTracker");

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
        if (followerState.State == TMasterStateManager::S_Following) {
            ++activeFollowerCount;
        }
    }
    return activeFollowerCount + 1 >= CellManager->GetQuorum();
}

void TFollowerStateTracker::ClearFollowerState(TFollowerState& followerState)
{
    followerState.State = TMasterStateManager::S_Stopped;
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

////////////////////////////////////////////////////////////////////////////////

} // namespace
