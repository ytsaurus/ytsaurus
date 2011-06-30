#pragma once

#include "master_state_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFollowerStateTracker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFollowerStateTracker> TPtr;

    struct TConfig
    {
        TDuration PingTimeout;

        TConfig()
            : PingTimeout(TDuration::MilliSeconds(3000))
        { }
    };

    TFollowerStateTracker(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr epochInvoker,
        IInvoker::TPtr serviceInvoker);

    bool HasActiveQuorum();

    void ProcessPing(TMasterId followerId, TMasterStateManager::EState state)
    {
        TFollowerState& followerState = FollowerStates[followerId];
        followerState.State = state;
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

private:
    struct TFollowerState
    {
        TMasterStateManager::EState State;
        TLeaseManager::TLease Lease;
    };

    void ClearFollowerState(TFollowerState& followerState);
    void ClearFollowerStates();
    void OnLeaseExpired(TMasterId followerId);

    TConfig Config;
    TCellManager::TPtr CellManager;
    IInvoker::TPtr EpochInvoker;
    IInvoker::TPtr ServiceInvoker;
    yvector<TFollowerState> FollowerStates;
    TLeaseManager::TPtr LeaseManager;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
