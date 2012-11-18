#include "stdafx.h"
#include "follower_tracker.h"
#include "private.h"
#include "config.h"
#include "decorated_meta_state.h"
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

TFollowerTracker::TFollowerTracker(
    TFollowerPingerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedMetaStatePtr decoratedState,
    const TEpochId& epoch,
    IInvokerPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedState(decoratedState)
    , EpochId(epoch)
    , EpochControlInvoker(epochControlInvoker)
    , ActiveQuorumPromise(NewPromise<void>())
{
    YCHECK(config);
    YCHECK(cellManager);
    YCHECK(decoratedState);
    YCHECK(epochControlInvoker);
    VERIFY_INVOKER_AFFINITY(epochControlInvoker, ControlThread);
}

void TFollowerTracker::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        if (id == CellManager->GetSelfId()) {
            Statuses.push_back(EPeerStatus::Leading);
        } else {
            Statuses.push_back(EPeerStatus::Stopped);
            SendPing(id);
        }
    }

    ActivePeerCount = 0;

    OnPeerActive(CellManager->GetSelfId());
}

bool TFollowerTracker::IsPeerActive(TPeerId peerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    auto status = Statuses[peerId];
    return status == EPeerStatus::Leading ||
        status == EPeerStatus::Following;
}

TFuture<void> TFollowerTracker::GetActiveQuorum()
{
    return ActiveQuorumPromise;
}

void TFollowerTracker::OnPeerActive(TPeerId peerId)
{
    ++ActivePeerCount;
    if (ActivePeerCount == CellManager->GetQuorum()) {
        ActiveQuorumPromise.Set();
    }
}

void TFollowerTracker::SendPing(TPeerId followerId)
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
        BIND(&TFollowerTracker::OnPingResponse, MakeStrong(this), followerId)
            .Via(EpochControlInvoker));       
}

void TFollowerTracker::SchedulePing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TDelayedInvoker::Submit(
        BIND(&TFollowerTracker::SendPing, MakeStrong(this), followerId)
            .Via(EpochControlInvoker),
        Config->PingInterval);
}

void TFollowerTracker::OnPingResponse(TPeerId followerId, TProxy::TRspPingFollowerPtr response)
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

    if (status == EPeerStatus::Following && Statuses[followerId] != EPeerStatus::Following) {
        OnPeerActive(followerId);
    }

    Statuses[followerId] = status;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NMetaState
} // namespace NYT
