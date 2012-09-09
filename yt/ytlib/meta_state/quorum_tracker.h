#pragma once

#include "public.h"
#include "meta_state_manager.h"

#include <ytlib/misc/thread_affinity.h>
#include <ytlib/misc/lease_manager.h>

#include <ytlib/actions/signal.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TQuorumTracker
    : public TRefCounted
{
public:
    TQuorumTracker(
        NElection::TCellManagerPtr cellManager,
        IInvokerPtr epochControlInvoker);

    bool HasActiveQuorum() const;
    bool IsPeerActive(TPeerId followerId) const;
    void SetStatus(TPeerId followerId, EPeerStatus status);

    TFuture<void> GetActiveQuorum();

private:
    void OnPeerActive(TPeerId peerId);

    NElection::TCellManagerPtr CellManager;
    IInvokerPtr EpochControlInvoker;
    std::vector<EPeerStatus> Statuses;
    int ActivePeerCount;
    TPromise<void> ActiveQuorumPromise;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
