#include "stdafx.h"
#include "follower_pinger.h"
#include "common.h"
#include "config.h"
#include "decorated_meta_state.h"
#include "snapshot_store.h"
#include "follower_tracker.h"
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
    TFollowerTrackerPtr followerTracker,
    const TEpoch& epoch,
    IInvoker::TPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , FollowerTracker(followerTracker)
    , Epoch(epoch)
    , EpochControlInvoker(epochControlInvoker)
{
    YASSERT(config);
    YASSERT(cellManager);
    YASSERT(decoratedState);
    YASSERT(followerTracker);
    YASSERT(epochControlInvoker);
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

    LOG_DEBUG("Sending ping to follower %d (Version: %s, Epoch: %s)",
        followerId,
        ~version.ToString(),
        ~Epoch.ToString());

    auto request = CellManager
        ->GetMasterProxy<TProxy>(followerId)
        ->PingFollower()
        ->SetTimeout(Config->RpcTimeout);
    request->set_segment_id(version.SegmentId);
    request->set_record_count(version.RecordCount);
    *request->mutable_epoch() = Epoch.ToProto();
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

void TFollowerPinger::OnPingResponse(TPeerId followerId, TProxy::TRspPingFollower::TPtr response)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SchedulePing(followerId);

    if (!response->IsOK()) {
        LOG_WARNING("Error pinging follower %d\n%s",
            followerId,
            ~response->GetError().ToString());
        return;
    }

    auto status = EPeerStatus(response->status());
    LOG_DEBUG("Ping reply received from follower %d (Status: %s)",
        followerId,
        ~status.ToString());
    FollowerTracker->ProcessPing(followerId, status);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
