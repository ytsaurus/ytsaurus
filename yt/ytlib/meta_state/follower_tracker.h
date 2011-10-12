#pragma once

#include "common.h"
#include "cell_manager.h"

#include "../actions/invoker_util.h"
#include "../misc/thread_affinity.h"
#include "../misc/lease_manager.h"

namespace NYT {
namespace NMetaState {

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
    void ProcessPing(TPeerId followerId, EPeerStatus status);

private:
    struct TFollowerState
    {
        EPeerStatus Status;
        TLeaseManager::TLease Lease;
    };

    void ChangeFollowerStatus(int followerId, EPeerStatus  status);
    void ResetFollowerState(int followerId);
    void ResetFollowerStates();
    void OnLeaseExpired(TPeerId followerId);

    TConfig Config;
    TCellManager::TPtr CellManager;
    TCancelableInvoker::TPtr EpochInvoker;
    yvector<TFollowerState> FollowerStates;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
