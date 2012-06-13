#pragma once

#include "public.h"
#include "meta_state_manager.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/lease_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TFollowerTracker
    : public TRefCounted
{
public:
    TFollowerTracker(
        TFollowerTrackerConfig* config,
        NElection::TCellManager* cellManager,
        IInvokerPtr epochControlInvoker);

    void Start();
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
    void OnLeaseExpired(TPeerId followerId);

    TFollowerTrackerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IInvokerPtr EpochControlInvoker;
    std::vector<TFollowerState> FollowerStates;
    int ActiveFollowerCount;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
