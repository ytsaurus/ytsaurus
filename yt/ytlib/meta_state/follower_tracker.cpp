#include "follower_tracker.h"

namespace NYT {
namespace NMetaState {

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
{
    YASSERT(~cellManager != NULL);
    YASSERT(~serviceInvoker != NULL);

    FollowerStates = yvector<TFollowerState>(cellManager->GetPeerCount());

    ResetFollowerStates();
}

void TFollowerTracker::Stop()
{
    EpochInvoker->Cancel();
}

bool TFollowerTracker::IsFollowerActive(TPeerId followerId)
{
    return FollowerStates[followerId].Status == EPeerStatus::Following;
}

bool TFollowerTracker::HasActiveQuorum()
{
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
    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        ResetFollowerState(id);
    }
}

void TFollowerTracker::ResetFollowerState(int followerId)
{
    ChangeFollowerStatus(followerId, EPeerStatus::Stopped);
    FollowerStates[followerId].Lease = TLeaseManager::TLease();
}

void TFollowerTracker::ChangeFollowerStatus(int followerId, EPeerStatus status)
{
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
    ResetFollowerState(followerId);
}

void TFollowerTracker::ProcessPing(TPeerId followerId, EPeerStatus status)
{
    ChangeFollowerStatus(followerId, status);

    auto& followerState = FollowerStates[followerId];
    if (followerState.Lease == TLeaseManager::TLease()) {
        followerState.Lease = TLeaseManager::Get()->CreateLease(
            Config.PingTimeout,
            FromMethod(&TFollowerTracker::OnLeaseExpired, TPtr(this), followerId)
            ->Via(~EpochInvoker));
    } else {
        TLeaseManager::Get()->RenewLease(followerState.Lease);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
