#include "stdafx.h"
#include "follower_pinger.h"

#include "../misc/serialize.h"
#include "../bus/message.h"

namespace NYT {
namespace NMetaState {

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerPinger::TFollowerPinger(
    const TConfig& config,
    TDecoratedMetaState::TPtr metaState,
    TCellManager::TPtr cellManager,
    TFollowerTracker::TPtr followerTracker,
    TSnapshotStore::TPtr snapshotStore,
    const TEpoch& epoch,
    IInvoker::TPtr controlInvoker)
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

    YASSERT(~FollowerTracker != NULL);
    YASSERT(~SnapshotStore != NULL);
    YASSERT(~CellManager != NULL);

    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TFollowerPinger::SendPing, TPtr(this))
        ->Via(MetaState->GetStateInvoker()),
        Config.PingInterval);
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

    auto version = MetaState->GetReachableVersion();
    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        auto proxy = CellManager->GetMasterProxy<TProxy>(peerId);
        proxy->SetTimeout(Config.RpcTimeout);
        auto request = proxy->PingFollower();
        request->SetSegmentId(version.SegmentId);
        request->SetRecordCount(version.RecordCount);
        request->SetEpoch(Epoch.ToProto());
        i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();
        request->SetMaxSnapshotId(maxSnapshotId);
        request->Invoke()->Subscribe(
            FromMethod(&TFollowerPinger::OnSendPing, TPtr(this), peerId)
            ->Via(ControlInvoker));
        
        LOG_DEBUG("Follower ping sent (FollowerId: %d, Version: %s, Epoch: %s, MaxSnapshotId: %d)",
            peerId,
            ~version.ToString(),
            ~Epoch.ToString(),
            maxSnapshotId);
    }

}

void TFollowerPinger::OnSendPing(TProxy::TRspPingFollower::TPtr response, TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (!response->IsOK()) {
        LOG_WARNING("Error pinging follower (FollowerId: %d, Error: %s)",
            followerId,
            ~response->GetError().ToString());
        return;
    }

    EPeerStatus status(response->GetStatus());
    LOG_DEBUG("Follower ping succeeded (FollowerId: %d, Status: %s)",
            followerId,
            ~status.ToString());
    FollowerTracker->ProcessPing(followerId, status);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
