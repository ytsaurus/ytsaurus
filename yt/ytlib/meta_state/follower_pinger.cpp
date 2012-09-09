#include "stdafx.h"
#include "follower_pinger.h"
#include "private.h"
#include "config.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "quorum_tracker.h"
#include "decorated_meta_state.h"

#include <ytlib/election/cell_manager.h>
#include <ytlib/misc/serialize.h>
#include <ytlib/bus/message.h>

namespace NYT {
namespace NMetaState {

using namespace NElection;

////////////////////////////////////////////////////////////////////////////////

static NLog::TLogger& Logger = MetaStateLogger;

////////////////////////////////////////////////////////////////////////////////

TFollowerPinger::TFollowerPinger(
    TFollowerPingerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    TQuorumTrackerPtr followerTracker,
    const TEpochId& epoch,
    IInvokerPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , QuorumTracker(followerTracker)
    , EpochId(epoch)
    , EpochControlInvoker(epochControlInvoker)
{
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(decoratedState);
    YCHECK(followerTracker);
    YCHECK(epochControlInvoker);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
}

void TFollowerPinger::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        if (id != CellManager->GetSelfId()) {
            SendPing(id);
        }
    }
}

void TFollowerPinger::Stop()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    // Do nothing.
}

void TFollowerPinger::SendPing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    
    auto version = DecoratedState->GetPingVersion();

    LOG_DEBUG("Sending ping to follower %d (Version: %s, EpochId: %s)",
        followerId,
        ~version.ToString(),
        ~EpochId.ToString());

    TProxy proxy(CellManager->GetMasterChannel(followerId));
    proxy.SetDefaultTimeout(Config->RpcTimeout);

    auto request = proxy.PingFollower();
    request->set_segment_id(version.SegmentId);
    request->set_record_count(version.RecordCount);
    *request->mutable_epoch_id() = EpochId.ToProto();
    request->Invoke().Subscribe(
        BIND(&TFollowerPinger::OnPingResponse, MakeStrong(this), followerId)
            .Via(EpochControlInvoker));       
}

void TFollowerPinger::SchedulePing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TDelayedInvoker::Submit(
        BIND(&TFollowerPinger::SendPing, MakeStrong(this), followerId)
            .Via(EpochControlInvoker),
        Config->PingInterval);
}

void TFollowerPinger::OnPingResponse(TPeerId followerId, TProxy::TRspPingFollowerPtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SchedulePing(followerId);

    if (!response->IsOK()) {
        LOG_WARNING(response->GetError(), "Error pinging follower %d",
            followerId);
        return;
    }

    auto status = EPeerStatus(response->status());
    LOG_DEBUG("Ping reply received from follower %d (Status: %s)",
        followerId,
        ~status.ToString());
    QuorumTracker->SetStatus(followerId, status);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
