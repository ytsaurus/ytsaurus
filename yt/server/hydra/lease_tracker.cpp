#include "stdafx.h"
#include "lease_tracker.h"
#include "private.h"
#include "config.h"
#include "decorated_automaton.h"

#include <core/concurrency/scheduler.h>

#include <ytlib/election/cell_manager.h>

namespace NYT {
namespace NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TLeaderLease::IsValid() const
{
    return NProfiling::GetCpuInstant() < Deadline_.load();
}

void TLeaderLease::SetDeadline(NProfiling::TCpuInstant deadline)
{
    YCHECK(Deadline_.load() < deadline);
    Deadline_ = deadline;
}

void TLeaderLease::Invalidate()
{
    Deadline_ = 0;
}

////////////////////////////////////////////////////////////////////////////////

class TLeaseTracker::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TLeaseTrackerPtr owner)
        : Owner_(owner)
        , Logger(Owner_->Logger)
    { }

    TFuture<void> Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        for (TPeerId id = 0; id < Owner_->CellManager_->GetPeerCount(); ++id) {
            if (id == Owner_->CellManager_->GetSelfPeerId()) {
                OnSuccess();
            } else {
                SendPing(id);
            }
        }

        Combine(AsyncResults_).Subscribe(
            BIND(&TFollowerPinger::OnComplete, MakeStrong(this))
                .Via(Owner_->EpochContext_->EpochControlInvoker));

        return Promise_;
    }

private:
    const TLeaseTrackerPtr Owner_;
    const NLogging::TLogger Logger;

    int ActiveCount_ = 0;
    std::vector<TFuture<void>> AsyncResults_;
    std::vector<TError> PingErrors_;

    TPromise<void> Promise_ = NewPromise<void>();


    void SendPing(TPeerId followerId)
    {
        auto channel = Owner_->CellManager_->GetPeerChannel(followerId);
        if (!channel)
            return;

        auto loggedVersion = Owner_->DecoratedAutomaton_->GetLoggedVersion();
        auto committedVersion = Owner_->DecoratedAutomaton_->GetCommittedVersion();

        LOG_DEBUG("Sending ping to follower (FollowerId: %v, LoggedVersion: %v, CommittedVersion: %v, EpochId: %v)",
            followerId,
            loggedVersion,
            committedVersion,
            Owner_->EpochContext_->EpochId);

        THydraServiceProxy proxy(channel);
        proxy.SetDefaultTimeout(Owner_->Config_->LeaderLeaseTimeout);

        auto req = proxy.PingFollower();
        ToProto(req->mutable_epoch_id(), Owner_->EpochContext_->EpochId);
        req->set_logged_revision(loggedVersion.ToRevision());
        req->set_committed_revision(committedVersion.ToRevision());
        AsyncResults_.push_back(req->Invoke().Apply(
            BIND(&TFollowerPinger::OnResponse, MakeStrong(this), followerId)
                .Via(Owner_->EpochContext_->EpochControlInvoker)));
    }

    void OnResponse(
        TPeerId followerId,
        const THydraServiceProxy::TErrorOrRspPingFollowerPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rspOrError.IsOK()) {
            PingErrors_.push_back(rspOrError);
            LOG_WARNING(rspOrError, "Error pinging follower (FollowerId: %v)",
                followerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto state = EPeerState(rsp->state());
        LOG_DEBUG("Follower ping succeded (FollowerId: %v, State: %v)",
            followerId,
            state);

        if (state == EPeerState::Following) {
            OnSuccess();
        }
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!Promise_.IsSet()) {
            auto error = TError("Could not acquire a quorum")
                << PingErrors_;
            Promise_.Set(error);
        }
    }

    void OnSuccess()
    {
        ++ActiveCount_;
        if (ActiveCount_ == Owner_->CellManager_->GetQuorumCount()) {
            Promise_.Set();
        }
    }

};

////////////////////////////////////////////////////////////////////////////////

TLeaseTracker::TLeaseTracker(
    TDistributedHydraManagerConfigPtr config,
    TCellManagerPtr cellManager,
    TDecoratedAutomatonPtr decoratedAutomaton,
    TEpochContext* epochContext,
    TLeaderLeasePtr lease,
    const std::vector<TCallback<TFuture<void>()>>& customLeaseCheckers)
    : Config_(config)
    , CellManager_(cellManager)
    , DecoratedAutomaton_(decoratedAutomaton)
    , EpochContext_(epochContext)
    , Lease_(lease)
    , CustomLeaseCheckers_(customLeaseCheckers)
{
    YCHECK(Config_);
    YCHECK(CellManager_);
    YCHECK(DecoratedAutomaton_);
    YCHECK(EpochContext_);
    YCHECK(Lease_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);

    Logger = HydraLogger;
    Logger.AddTag("CellId: %v", CellManager_->GetCellId());
}

void TLeaseTracker::Start()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LeaseCheckExecutor_ = New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaseTracker::OnLeaseCheck, MakeWeak(this)),
        Config_->LeaderLeaseCheckPeriod);
    LeaseCheckExecutor_->Start();

}

TFuture<void> TLeaseTracker::GetLeaseAcquired()
{
    return LeaseAcquired_;
}

TFuture<void> TLeaseTracker::GetLeaseLost()
{
    return LeaseLost_;
}

void TLeaseTracker::OnLeaseCheck()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    LOG_DEBUG("Starting leader lease check");

    auto startTime = NProfiling::GetCpuInstant();
    auto asyncResult = FireLeaseCheck();
    auto result = WaitFor(asyncResult);

    if (result.IsOK()) {
        Lease_->SetDeadline(startTime + NProfiling::DurationToCpuDuration(Config_->LeaderLeaseTimeout));
        LOG_DEBUG("Leader lease check succceded");
        if (!LeaseAcquired_.IsSet()) {
            LeaseAcquired_.Set();
        }
    } else {
        LOG_DEBUG(result, "Leader lease check failed");
        if (Lease_->IsValid() && LeaseAcquired_.IsSet() && !LeaseLost_.IsSet()) {
            Lease_->Invalidate();
            LeaseLost_.Set(result);
        }
    }
}

TFuture<void> TLeaseTracker::FireLeaseCheck()
{
    std::vector<TFuture<void>> asyncResults;

    auto pinger = New<TFollowerPinger>(this);
    asyncResults.push_back(pinger->Run());

    for (auto callback : CustomLeaseCheckers_) {
        asyncResults.push_back(callback.Run());
    }

    return Combine(asyncResults);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NHydra
} // namespace NYT
