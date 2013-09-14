#pragma once

#include "private.h"
#include "meta_state_manager_proxy.h"

#include <core/concurrency/periodic_executor.h>
#include <core/concurrency/thread_affinity.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TFollowerTracker
    : public TRefCounted
{
public:
    TFollowerTracker(
        TFollowerTrackerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        const TEpochId& epochId,
        IInvokerPtr epochControlInvoker);

    void Start();

    bool IsPeerActive(TPeerId followerId) const;

    TFuture<void> GetActiveQuorum();

private:
    typedef TMetaStateManagerProxy TProxy;

    TFollowerTrackerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TEpochId EpochId;
    IInvokerPtr EpochControlInvoker;

    std::vector<EPeerStatus> Statuses;
    int ActivePeerCount;
    TPromise<void> ActiveQuorumPromise;

    void SendPing(TPeerId followerId);
    void SchedulePing(TPeerId followerId);
    void OnPingResponse(TPeerId followerId, TProxy::TRspPingFollowerPtr response);
    void OnPeerActive(TPeerId peerId);

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
