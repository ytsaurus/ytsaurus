#include "stdafx.h"
#include "leader_pinger.h"

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
    const TEpoch& epoch)
    : Config(config)
    , MetaState(metaState)
    , CellManager(cellManager)
    , FollowerTracker(followerTracker)
    , SnapshotStore(snapshotStore)
    , Epoch(epoch)
{
    PeriodicInvoker = new TPeriodicInvoker(
        FromMethod(&TFollowerPinger::SendPing, TPtr(this))
        ->Via(MetaState->GetStateInvoker()),
        Config.PingInterval);
    PeriodicInvoker->Start();
}

void TFollowerPinger::Stop()
{
    PeriodicInvoker->Stop();
}

void TFollowerPinger::SendPing()
{
    VERIFY_THREAD_AFFINITY(StateThread);

    auto version = MetaState->GetReachableVersion();
    for (TPeerId peerId = 0; peerId < CellManager->GetPeerCount(); ++peerId) {
        if (peerId == CellManager->GetSelfId()) continue;

        auto proxy = CellManager->GetMasterProxy<TProxy>(peerId);
        auto request = proxy->PingFollower();
        request->SetSegmentId(version.SegmentId);
        request->SetRecordCount(version.RecordCount);
        request->SetEpoch(Epoch);
        request->SetMaxSnapshotId(SnapshotStore->GetMaxSnapshotId());
        request->Invoke(Config.RpcTimeout)->Subscribe
    }

    auto status = MetaStateManager->GetControlStatus();
    auto proxy = CellManager->GetMasterProxy<TProxy>(LeaderId);
    proxy->SetTimeout(Config.RpcTimeout);

    auto request = proxy->PingLeader();
    request->SetEpoch(Epoch.ToProto());
    request->SetFollowerId(CellManager->GetSelfId());
    request->SetStatus(status);
    request->Invoke()->Subscribe(
        FromMethod(
        &TLeaderPinger::OnSendPing, TPtr(this))
        ->Via(~CancelableInvoker));

    LOG_DEBUG("Leader ping sent (LeaderId: %d, State: %s)",
        LeaderId,
        ~status.ToString());
}

void TFollowerPinger::OnSendPing(TProxy::TRspPingLeader::TPtr response, TPeerId peerId)
{
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
