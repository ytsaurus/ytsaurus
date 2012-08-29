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
        TQuorumTrackerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr epochControlInvoker,
        TPeerId leaderId);

    bool HasActiveQuorum() const;
    bool IsFollowerActive(TPeerId followerId) const;
    void SetStatus(TPeerId followerId, EPeerStatus status);

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

    TQuorumTrackerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    IInvokerPtr EpochControlInvoker;
    std::vector<TPeerInfo> Peers;
    int ActivePeerCount;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
