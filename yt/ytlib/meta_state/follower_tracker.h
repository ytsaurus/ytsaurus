#pragma once

#include "common.h"
#include "cell_manager.h"

#include "../misc/lease_manager.h"
#include "../misc/thread_affinity.h"

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
    bool IsFollowerActive(TPeerId followerId);
    void ProcessPing(TPeerId followerId, EPeerState state);

private:
    struct TFollowerState
    {
        EPeerState State;
        TLeaseManager::TLease Lease;
    };

    void ChangeFollowerState(int followerId, EPeerState  state);
    void ResetFollowerState(int followerId);
    void ResetFollowerStates();
    void OnLeaseExpired(TPeerId followerId);

    TConfig Config;
    TCellManager::TPtr CellManager;
    TCancelableInvoker::TPtr EpochInvoker;
    yvector<TFollowerState> FollowerStates;
    TLeaseManager::TPtr LeaseManager;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
