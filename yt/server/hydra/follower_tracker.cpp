#include "stdafx.h"
#include "follower_tracker.h"
#include "private.h"
#include "config.h"
#include "decorated_automaton.h"

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

TFollowerTracker::TFollowerTracker(
    TFollowerTrackerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    const TEpochId& epoch,
    IInvokerPtr epochControlInvoker)
    : Config(config)
    , CellManager(cellManager)
    , DecoratedAutomaton(decoratedAutomaton)
    , EpochId(epoch)
    , EpochControlInvoker(epochControlInvoker)
    , ActivePeerCount(0)
    , ActiveQuorumPromise(NewPromise())
    , Logger(HydraLogger)
{
    YCHECK(Config);
    YCHECK(CellManager);
    YCHECK(DecoratedAutomaton);
    YCHECK(EpochControlInvoker);
    VERIFY_INVOKER_AFFINITY(EpochControlInvoker, ControlThread);

    Logger.AddTag(Sprintf("CellGuid: %s",
        ~ToString(CellManager->GetCellGuid())));

    for (TPeerId id = 0; id < CellManager->GetPeerCount(); ++id) {
        if (id == CellManager->GetSelfId()) {
            PeerStates.push_back(EPeerState::Leading);
        } else {
            PeerStates.push_back(EPeerState::Stopped);
            SendPing(id);
        }
    }
}

void TFollowerTracker::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    OnPeerActivated();
}

bool TFollowerTracker::IsFollowerActive(TPeerId peerId) const
{
    VERIFY_THREAD_AFFINITY_ANY();

    return PeerStates[peerId] == EPeerState::Following;
}

void TFollowerTracker::ResetFollower(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(followerId != CellManager->GetSelfId());

    SetFollowerState(followerId, EPeerState::Stopped);
}

TFuture<void> TFollowerTracker::GetActiveQuorum()
{
    return ActiveQuorumPromise;
}

void TFollowerTracker::SendPing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto channel = CellManager->GetPeerChannel(followerId);
    if (!channel) {
        SchedulePing(followerId);
        return;
    }

    auto loggedVersion = DecoratedAutomaton->GetLoggedVersion();
    auto committedVersion = DecoratedAutomaton->GetAutomatonVersion();

    LOG_DEBUG("Sending ping to follower %d (LoggedVersion: %s, CommittedVersion: %s, EpochId: %s)",
        followerId,
        ~ToString(loggedVersion),
        ~ToString(committedVersion),
        ~ToString(EpochId));

    THydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config->RpcTimeout);

    auto req = proxy.PingFollower();
    ToProto(req->mutable_epoch_id(), EpochId);
    req->set_logged_revision(loggedVersion.ToRevision());
    req->set_committed_revision(committedVersion.ToRevision());
    req->Invoke().Subscribe(
        BIND(&TFollowerTracker::OnPingResponse, MakeStrong(this), followerId)
            .Via(EpochControlInvoker));
}

void TFollowerTracker::SchedulePing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TDelayedExecutor::Submit(
        BIND(&TFollowerTracker::SendPing, MakeStrong(this), followerId)
            .Via(EpochControlInvoker),
        Config->PingInterval);
}

void TFollowerTracker::OnPingResponse(TPeerId followerId, THydraServiceProxy::TRspPingFollowerPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SchedulePing(followerId);

    if (!rsp->IsOK()) {
        LOG_WARNING(rsp->GetError(), "Error pinging follower %d",
            followerId);
        return;
    }

    auto state = EPeerState(rsp->state());
    LOG_DEBUG("Ping reply received from follower %d (State: %s)",
        followerId,
        ~ToString(state));

    SetFollowerState(followerId, state);
}

void TFollowerTracker::SetFollowerState(TPeerId followerId, EPeerState state)
{
    auto oldState = PeerStates[followerId];
    if (oldState == state)
        return;

    LOG_INFO("Follower %d state changed: %s->%s",
        followerId,
        ~ToString(oldState),
        ~ToString(state));

    if (state == EPeerState::Following && oldState != EPeerState::Following) {
        OnPeerActivated();
    }

    if (state != EPeerState::Following && oldState == EPeerState::Following) {
        OnPeerDeactivated();
    }

    PeerStates[followerId] = state;
}

void TFollowerTracker::OnPeerActivated()
{
    ++ActivePeerCount;
    if (!ActiveQuorumPromise.IsSet() && ActivePeerCount >= CellManager->GetQuorumCount()) {
        ActiveQuorumPromise.Set();
    }
}

void TFollowerTracker::OnPeerDeactivated()
{
    --ActivePeerCount;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
