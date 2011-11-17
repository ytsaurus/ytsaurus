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
    IInvoker::TPtr serviceInvoker)
    : Config(config)
    , MetaState(metaState)
    , CellManager(cellManager)
    , FollowerTracker(followerTracker)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
    , CancelableInvoker(New<TCancelableInvoker>(serviceInvoker))
{
    VERIFY_INVOKER_AFFINITY(serviceInvoker, ControlThread);
    VERIFY_INVOKER_AFFINITY(MetaState->GetStateInvoker(), StateThread);

    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TFollowerPinger::SendPing, TPtr(this))
        ->Via(MetaState->GetStateInvoker()),
        Config.PingInterval);
    PeriodicInvoker->Start();
}

void TFollowerPinger::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    CancelableInvoker->Cancel();
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
        request->SetEpoch(Epoch);
        i32 maxSnapshotId = SnapshotStore->GetMaxSnapshotId();
        request->SetMaxSnapshotId(maxSnapshotId);
        request->Invoke()->Subscribe(
            FromMethod(&TFollowerPinger::OnSendPing, TPtr(this), peerId)
            ->Via(CancelableInvoker));
        
        LOG_DEBUG("Follower ping sent (FollowerId: %d, Version: %s, Epoch: %s, MaxSnapshotId: %d)",
            peerId,
            ~version.ToString(),
            ~Epoch.ToString(),
            maxSnapshotId);
    }

}

void TFollowerPinger::OnSendPing(TProxy::TRspPingLeader::TPtr response, TPeerId peerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (response->IsOK()) {
        LOG_DEBUG("Leader ping succeeded (LeaderId: %d)",
            LeaderId);
    } else {
        LOG_WARNING("Error pinging leader (LeaderId: %d, Error: %s)",
            LeaderId,
            ~response->GetError().ToString());
    }

    if (response->GetErrorCode() == NRpc::EErrorCode::Timeout) {
        SendPing();
    } else {
        SchedulePing();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
