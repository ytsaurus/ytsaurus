#pragma once

#include "common.h"
#include "meta_state_manager.h"
#include "meta_state_manager_proxy.h"
#include "cell_manager.h"
#include "follower_tracker.h"
#include "snapshot_store.h"
#include "decorated_meta_state.h"

#include <ytlib/misc/periodic_invoker.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

DECLARE_ENUM(EFollowerPingerMode,
    (Recovery)
    (Leading)
);

class TFollowerPinger
    : public TRefCounted
{
public:
    typedef TIntrusivePtr<TFollowerPinger> TPtr;

    TFollowerPinger(
        EFollowerPingerMode mode,
        TFollowerPingerConfig* config,
        TDecoratedMetaState* metaState,
        TCellManager* cellManager,
        TFollowerTracker* followerTracker,
        TSnapshotStore* snapshotStore,
        const TEpoch& epoch,
        IInvoker* epochControlInvoker,
        IInvoker* epochStateInvoker);

    void Start();
    void Stop();

private:
    typedef TMetaStateManagerProxy TProxy;

    void SendPing();
    void OnPingReply(TProxy::TRspPingFollower::TPtr response, TPeerId followerId);

    EFollowerPingerMode Mode;
    TFollowerPingerConfigPtr Config;
    TPeriodicInvoker::TPtr PeriodicInvoker;
    TDecoratedMetaStatePtr MetaState;
    TCellManagerPtr CellManager;
    TFollowerTrackerPtr FollowerTracker;
    TSnapshotStorePtr SnapshotStore;
    TEpoch Epoch;
    IInvoker::TPtr EpochControlInvoker;
    IInvoker::TPtr EpochStateInvoker;
    TMetaVersion ReachableVersion;

    DECLARE_THREAD_AFFINITY_SLOT(ControlThread);
    DECLARE_THREAD_AFFINITY_SLOT(StateThread);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
