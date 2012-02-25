#include "stdafx.h"
#include "follower_pinger.h"

#include <ytlib/misc/serialize.h>
#include <ytlib/bus/message.h>

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerPinger::TFollowerPinger(
    TConfig* config,
    TDecoratedMetaState* metaState,
    TCellManager* cellManager,
    TFollowerTracker* followerTracker,
    TSnapshotStore* snapshotStore,
    const TEpoch& epoch,
    IInvoker* controlInvoker)
    : Config(config)
    , MetaState(metaState)
    , CellManager(cellManager)
    , FollowerTracker(followerTracker)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , ControlInvoker(New<TCancelableInvoker>(controlInvoker))
{
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(MetaState->GetStateInvoker(), StateThread);

    YASSERT(FollowerTracker);
    YASSERT(SnapshotStore);
    YASSERT(CellManager);

    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TFollowerPinger::SendPing, TPtr(this))
        ->Via(MetaState->GetStateInvoker()),
        Config->PingInterval);
    PeriodicInvoker->Start();
}

void TFollowerPinger::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ControlInvoker->Cancel();
    PeriodicInvoker->Stop();
}

void TFollowerPinger::SendPing()
{
    VERIFY_THREAD_AFFINITY(StateThread);

	auto version = MetaState->SafeGetReachableVersion();

    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        auto request =
            CellManager->GetMasterProxy<TProxy>(peerId)
            ->PingFollower()
            ->SetTimeout(Config->RpcTimeout);
        request->set_segment_id(version.SegmentId);
        request->set_record_count(version.RecordCount);
        request->set_epoch(Epoch.ToProto());
        i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();
        request->set_max_snapshot_id(maxSnapshotId);
        request->Invoke()->Subscribe(
            FromMethod(&TFollowerPinger::OnPingReply, TPtr(this), peerId)
            ->Via(ControlInvoker));
        
        LOG_DEBUG("Sent ping to follower %d (Version: %s, Epoch: %s, MaxSnapshotId: %d)",
            peerId,
            ~version.ToString(),
            ~Epoch.ToString(),
            maxSnapshotId);
    }

}

void TFollowerPinger::OnPingReply(TProxy::TRspPingFollower::TPtr response, TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!response->IsOK()) {
        LOG_WARNING("Error pinging follower (FollowerId: %d)\n%s",
            followerId,
            ~response->GetError().ToString());
        return;
    }

    EPeerStatus status(response->status());
    LOG_DEBUG("Follower ping succeeded (FollowerId: %d, Status: %s)",
            followerId,
            ~status.ToString());
    FollowerTracker->ProcessPing(followerId, status);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
