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
    EFollowerPingerMode mode,
    TConfig* config,
    TDecoratedMetaState* metaState,
    TCellManager* cellManager,
    TFollowerTracker* followerTracker,
    TSnapshotStore* snapshotStore,
    const TEpoch& epoch,
    IInvoker* controlInvoker)
    : Mode(mode)
    , Config(config)
    , MetaState(metaState)
    , CellManager(cellManager)
    , FollowerTracker(followerTracker)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , ControlInvoker(New<TCancelableInvoker>(controlInvoker))
    , StateInvoker(New<TCancelableInvoker>(MetaState->GetStateInvoker()))
{
    VERIFY_INVOKER_AFFINITY(controlInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(MetaState->GetStateInvoker(), StateThread);

    YASSERT(followerTracker);
    YASSERT(snapshotStore);
    YASSERT(cellManager);
}

void TFollowerPinger::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    switch (Mode) {
        case EFollowerPingerMode::Recovery:
            ReachableVersion = MetaState->GetReachableVersionAsync();
            PeriodicInvoker = new TPeriodicInvoker(
                FromMethod(&TFollowerPinger::SendPing, TPtr(this))->Via(ControlInvoker),
                Config->PingInterval);
            break;

        case EFollowerPingerMode::Leading:
            PeriodicInvoker = new TPeriodicInvoker(
                FromMethod(&TFollowerPinger::SendPing, TPtr(this))->Via(MetaState->GetStateInvoker()),
                Config->PingInterval);
            break;

        default:
            YUNREACHABLE();
    }

    PeriodicInvoker->Start();
}

void TFollowerPinger::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    ControlInvoker->Cancel();
    StateInvoker->Cancel();
    PeriodicInvoker->Stop();
}

void TFollowerPinger::SendPing()
{
    auto version =
        Mode == EFollowerPingerMode::Recovery
        ? ReachableVersion
        : MetaState->GetReachableVersion();

    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        auto request = CellManager
            ->GetMasterProxy<TProxy>(peerId)
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
        LOG_WARNING("Error pinging follower %d\n%s",
            followerId,
            ~response->GetError().ToString());
        return;
    }

    EPeerStatus status(response->status());
    LOG_DEBUG("Ping reply received from Follower %d (Status: %s)",
        followerId,
        ~status.ToString());
    FollowerTracker->ProcessPing(followerId, status);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
