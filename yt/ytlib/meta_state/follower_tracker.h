#pragma once

#include "common.h"
#include "cell_manager.h"
#include "meta_state_manager.h"

#include <ytlib/actions/invoker_util.h>
#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/lease_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TFollowerTracker
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TFollowerTracker> TPtr;

    struct TConfig
        : public TConfigurable
    {
        typedef TIntrusivePtr<TConfig> TPtr;

        TDuration PingTimeout;

        TConfig()
        {
            Register("ping_timeout", PingTimeout)
                .GreaterThan(TDuration())
                .Default(TDuration::MilliSeconds(3000));
        }
    };

    TFollowerTracker(
        TConfig* config,
        TCellManager::TPtr cellManager,
        IInvoker::TPtr serviceInvoker);

    void Stop();
    bool HasActiveQuorum() const;
    bool IsFollowerActive(TPeerId followerId) const;
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

    TConfig::TPtr Config;
    TCellManager::TPtr CellManager;
    TCancelableInvoker::TPtr EpochInvoker;
    yvector<TFollowerState> FollowerStates;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
