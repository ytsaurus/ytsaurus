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
    TEpochContext* epochContext)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochContext_(epochContext)
    , Logger(HydraLogger)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochContext_);
    VERIFY_INVOKER_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);

    Logger.AddTag("CellId: %v", CellManager_->GetCellId());

    for (TPeerId id = 0; id < CellManager_->GetPeerCount(); ++id) {
        if (id == CellManager_->GetSelfPeerId()) {
            PeerStates_.push_back(EPeerState::Leading);
        } else {
            PeerStates_.push_back(EPeerState::Stopped);
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

    return PeerStates_[peerId] == EPeerState::Following;
}

void TFollowerTracker::ResetFollower(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);
    YCHECK(followerId != CellManager_->GetSelfPeerId());

    SetFollowerState(followerId, EPeerState::Stopped);
}

TFuture<void> TFollowerTracker::GetActiveQuorum()
{
    return ActiveQuorumPromise_;
}

void TFollowerTracker::SendPing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto channel = CellManager_->GetPeerChannel(followerId);
    if (!channel) {
        SchedulePing(followerId);
        return;
    }

    auto loggedVersion = DecoratedAutomaton_->GetLoggedVersion();
    auto committedVersion = DecoratedAutomaton_->GetAutomatonVersion();

    LOG_DEBUG("Sending ping to follower %v (LoggedVersion: %v, CommittedVersion: %v, EpochId: %v)",
        followerId,
        loggedVersion,
        committedVersion,
        EpochContext_->EpochId);

    THydraServiceProxy proxy(channel);
    proxy.SetDefaultTimeout(Config_->RpcTimeout);

    auto req = proxy.PingFollower();
    ToProto(req->mutable_epoch_id(), EpochContext_->EpochId);
    req->set_logged_revision(loggedVersion.ToRevision());
    req->set_committed_revision(committedVersion.ToRevision());
    req->Invoke().Subscribe(
        BIND(&TFollowerTracker::OnPingResponse, MakeStrong(this), followerId)
            .Via(EpochContext_->EpochControlInvoker));
}

void TFollowerTracker::SchedulePing(TPeerId followerId)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TDelayedExecutor::Submit(
        BIND(&TFollowerTracker::SendPing, MakeStrong(this), followerId)
            .Via(EpochContext_->EpochControlInvoker),
        Config_->PingInterval);
}

void TFollowerTracker::OnPingResponse(TPeerId followerId, THydraServiceProxy::TRspPingFollowerPtr rsp)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    SchedulePing(followerId);

    if (!rsp->IsOK()) {
        LOG_WARNING(rsp->GetError(), "Error pinging follower %v",
            followerId);
        return;
    }

    auto state = EPeerState(rsp->state());
    LOG_DEBUG("Ping reply received from follower %v (State: %v)",
        followerId,
        state);

    SetFollowerState(followerId, state);
}

void TFollowerTracker::SetFollowerState(TPeerId followerId, EPeerState newState)
{
    auto oldState = PeerStates_[followerId];
    if (oldState == newState)
        return;

    LOG_INFO("Follower %v state changed: %v -> %v",
        followerId,
        oldState,
        newState);

    if (newState == EPeerState::Following && oldState != EPeerState::Following) {
        OnPeerActivated();
    }

    if (newState != EPeerState::Following && oldState == EPeerState::Following) {
        OnPeerDeactivated();
    }

    PeerStates_[followerId] = newState;
}

void TFollowerTracker::OnPeerActivated()
{
    if (++ActivePeerCount_ >= CellManager_->GetQuorumCount()) {
        // This can happen more than once.
        ActiveQuorumPromise_.TrySet();
    }
}

void TFollowerTracker::OnPeerDeactivated()
{
    YCHECK(--ActivePeerCount_ >= 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
