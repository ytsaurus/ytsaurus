#pragma once

#include "master_state_manager.h"

#include "../misc/lease_manager.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

class TFollowerTracker
    : public TRefCountedBase
{
public:
    typedef TIntrusivePtr<TFollowerTracker> TPtr;

    struct TConfig
    {
        TDuration PingTimeout;

        TConfig()
            : PingTimeout(TDuration::MilliSeconds(3000))
        { }
    };

    TFollowerTracker(
        const TConfig& config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr serviceInvoker);

    void Stop();
    bool HasActiveQuorum();
    void ProcessPing(TPeerId followerId, TMetaStateManager::EState state);

private:
    struct TFollowerState
    {
        TMetaStateManager::EState State;
        TLeaseManager::TLease Lease;
    };

    void ChangeFollowerState(
        int followerId,
        TMetaStateManager::EState state);
    void ResetFollowerState(int followerId);
    void ResetFollowerStates();
    void OnLeaseExpired(TPeerId followerId);

    TConfig Config;
    TCellManager::TPtr CellManager;
    TCancelableInvoker::TPtr EpochInvoker;
    yvector<TFollowerState> FollowerStates;
    TLeaseManager::TPtr LeaseManager;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
