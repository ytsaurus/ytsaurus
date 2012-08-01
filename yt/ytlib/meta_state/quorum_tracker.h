#pragma once

#include "public.h"
#include "meta_state_manager.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/lease_manager.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TQuorumTracker
    : public TRefCounted
{
public:
    TQuorumTracker(
        TFollowerTrackerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr epochControlInvoker);

    void Start(TPeerId leaderId);
    void Stop();
    bool HasActiveQuorum() const;
    bool IsFollowerActive(TPeerId followerId) const;
    void ProcessPing(TPeerId followerId, EPeerStatus status);

private:
    struct TPeerInfo
    {
        EPeerStatus Status;
        TLeaseManager::TLease Lease;
    };

    void ChangeFollowerStatus(int followerId, EPeerStatus  status);
    void ResetFollowerState(int followerId);
    void OnLeaseExpired(TPeerId followerId);
    void UpdateActiveQuorum();

    TFollowerTrackerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IInvokerPtr EpochControlInvoker;
    std::vector<TPeerInfo> Peers;
    int ActivePeerCount;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
