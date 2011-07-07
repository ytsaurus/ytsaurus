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
    void ProcessPing(TMasterId followerId, TMasterStateManager::EState state);

private:
    struct TFollowerState
    {
        TMasterStateManager::EState State;
        TLeaseManager::TLease Lease;
    };

    void ChangeFollowerState(
        int followerId,
        TMasterStateManager::EState state);
    void ResetFollowerState(int followerId);
    void ResetFollowerStates();
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
