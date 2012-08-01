#pragma once

#include "private.h"
#include "meta_state_manager_proxy.h"

#include <ytlib/misc/periodic_invoker.h>
#include <ytlib/misc/thread_affinity.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

class TFollowerPinger
    : public TRefCounted
{
public:
    TFollowerPinger(
        TFollowerPingerConfigPtr config,
        NElection::TCellManagerPtr cellManager,
        TDecoratedMetaStatePtr decoratedState,
        TQuorumTrackerPtr followerTracker,
        const TEpoch& epoch,
        IInvokerPtr epochControlInvoker);

    void Start();
    void Stop();

private:
    typedef TMetaStateManagerProxy TProxy;

    void SendPing(TPeerId followerId);
    void SchedulePing(TPeerId followerId);
    void OnPingResponse(TPeerId followerId, TProxy::TRspPingFollowerPtr response);

    TFollowerPingerConfigPtr Config;
    NElection::TCellManagerPtr CellManager;
    TDecoratedMetaStatePtr DecoratedState;
    TQuorumTrackerPtr QuorumTracker;
    TSnapshotStorePtr SnapshotStore;
    TEpoch Epoch;
    IInvokerPtr EpochControlInvoker;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
