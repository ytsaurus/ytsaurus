#include "lease_tracker.h"
#include "private.h"
#include "decorated_automaton.h"
#include "hydra_service_proxy.h"
#include "config.h"

#include <yt/yt/ytlib/election/cell_manager.h>
#include <yt/yt/ytlib/election/config.h>

#include <yt/yt/core/concurrency/scheduler.h>

namespace NYT::NHydra {

using namespace NElection;
using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

bool TLeaderLease::IsValid() const
{
    return NProfiling::GetCpuInstant() < Deadline_.load();
}

void TLeaderLease::Restart()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    Deadline_.store(NotAcquiredDeadline);
}

void TLeaderLease::Extend(NProfiling::TCpuInstant deadline)
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    auto curDeadline = Deadline_.load();
    if (curDeadline == AbandonedDeadline) {
        return;
    }
    YT_VERIFY(curDeadline < deadline);
    Deadline_.store(deadline);
}

bool TLeaderLease::TryAbandon()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    if (NProfiling::GetCpuInstant() >= Deadline_.load()) {
        return false;
    }
    Deadline_.store(AbandonedDeadline);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

// Also pings non-voting peers.
class TLeaseTracker::TFollowerPinger
    : public TRefCounted
{
public:
    explicit TFollowerPinger(TLeaseTrackerPtr owner)
        : Owner_(std::move(owner))
        , Logger(Owner_->Logger)
    { }

    TFuture<void> Run()
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        for (TPeerId id = 0; id < Owner_->EpochContext_->CellManager->GetTotalPeerCount(); ++id) {
            if (id == Owner_->EpochContext_->CellManager->GetSelfPeerId()) {
                OnSuccess();
            } else {
                SendPing(id);
            }
        }

        AllSucceeded(AsyncResults_).Subscribe(
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

    const TPromise<void> Promise_ = NewPromise<void>();


    void SendPing(TPeerId followerId)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        const auto& epochContext = Owner_->EpochContext_;

        auto channel = epochContext->CellManager->GetPeerChannel(followerId);
        if (!channel) {
            return;
        }

        auto alivePeerIds = epochContext->AlivePeerIds.Load();

        YT_LOG_DEBUG("Sending ping to follower (FollowerId: %v, Term: %v, EpochId: %v, AlivePeerIds: %v)",
            followerId,
            epochContext->Term,
            epochContext->EpochId,
            alivePeerIds);

        TInternalHydraServiceProxy proxy(channel);
        auto req = proxy.PingFollower();
        req->SetTimeout(Owner_->Config_->LeaderLeaseTimeout);
        ToProto(req->mutable_epoch_id(), epochContext->EpochId);
        if (Owner_->TermSendingEnabled_) {
            req->set_term(epochContext->Term);
        }
        for (auto peerId : alivePeerIds) {
            req->add_alive_peer_ids(peerId);
        }

        bool voting = Owner_->EpochContext_->CellManager->GetPeerConfig(followerId)->Voting;
        AsyncResults_.push_back(req->Invoke().Apply(
            BIND(&TFollowerPinger::OnResponse, MakeStrong(this), followerId, voting)
                .Via(epochContext->EpochControlInvoker)));
    }

    void OnResponse(
        TPeerId followerId,
        bool voting,
        const TInternalHydraServiceProxy::TErrorOrRspPingFollowerPtr& rspOrError)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!rspOrError.IsOK()) {
            PingErrors_.push_back(rspOrError);
            YT_LOG_WARNING(rspOrError, "Error pinging follower (PeerId: %v)",
                followerId);
            return;
        }

        const auto& rsp = rspOrError.Value();
        auto state = EPeerState(rsp->state());
        YT_LOG_DEBUG("Follower ping succeeded (PeerId: %v, State: %v)",
            followerId,
            state);

        if (voting) {
            if (state == EPeerState::Following || state == EPeerState::FollowerRecovery) {
                OnSuccess();
            } else {
                PingErrors_.push_back(TError("Follower %v is in %Qlv state",
                    followerId,
                    state));
            }
        }
    }

    void OnComplete(const TError&)
    {
        VERIFY_THREAD_AFFINITY(Owner_->ControlThread);

        if (!Promise_.IsSet()) {
            auto error = TError("Could not acquire quorum")
                << PingErrors_;
            Promise_.Set(error);
        }
    }

    void OnSuccess()
    {
        ++ActiveCount_;
        if (ActiveCount_ == Owner_->EpochContext_->CellManager->GetQuorumPeerCount()) {
            Promise_.Set();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TLeaseTracker::TLeaseTracker(
    NHydra::TDistributedHydraManagerConfigPtr config,
    TEpochContext* epochContext,
    TLeaderLeasePtr lease,
    std::vector<TCallback<TFuture<void>()>> customLeaseCheckers,
    NLogging::TLogger logger)
    : Config_(std::move(config))
    , EpochContext_(epochContext)
    , Lease_(std::move(lease))
    , CustomLeaseCheckers_(std::move(customLeaseCheckers))
    , Logger(std::move(logger))
    , LeaseCheckExecutor_(New<TPeriodicExecutor>(
        EpochContext_->EpochControlInvoker,
        BIND(&TLeaseTracker::OnLeaseCheck, MakeWeak(this)),
        Config_->LeaderLeaseCheckPeriod))
{
    YT_VERIFY(Config_);
    YT_VERIFY(EpochContext_);
    VERIFY_INVOKER_THREAD_AFFINITY(EpochContext_->EpochControlInvoker, ControlThread);

    LeaseCheckExecutor_->Start();
}

void TLeaseTracker::EnableTracking()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(TermSendingEnabled_);

    Lease_->Restart();
    TrackingEnabled_ = true;
}

void TLeaseTracker::EnableSendingTerm()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    TermSendingEnabled_ = true;
}

void TLeaseTracker::Finalize()
{
    YT_UNUSED_FUTURE(LeaseCheckExecutor_->Stop());

    auto error = TError("Hydra instance is finalizing");
    NextCheckPromise_.TrySet(error);

    Finalized_ = true;
}

TFuture<void> TLeaseTracker::GetNextQuorumFuture()
{
    auto result =
        BIND([=, this, this_ = MakeStrong(this)] {
            VERIFY_THREAD_AFFINITY(ControlThread);

            while (!Finalized_) {
                auto future = NextCheckPromise_.ToFuture();
                auto error = WaitFor(future);
                if (error.IsOK()) {
                    break;
                }
            }
        })
        .AsyncVia(EpochContext_->EpochControlInvoker)
        .Run();

    LeaseCheckExecutor_->ScheduleOutOfBand();

    return result;
}

void TLeaseTracker::SubscribeLeaseLost(const TCallback<void(const TError&)>& callback)
{
    VERIFY_THREAD_AFFINITY_ANY();

    LeaseLost_.Subscribe(callback);
}

void TLeaseTracker::OnLeaseCheck()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    YT_VERIFY(!Finalized_);

    auto startTime = NProfiling::GetCpuInstant();
    auto trackingEnabled = TrackingEnabled_;
    auto checkPromise = std::move(NextCheckPromise_);
    NextCheckPromise_ = NewPromise<void>();

    YT_LOG_DEBUG("Starting leader lease check (TrackingEnabled: %v)",
        trackingEnabled);

    auto checkResult = WaitFor(FireLeaseCheck());
    if (checkResult.IsOK()) {
        YT_LOG_DEBUG("Leader lease check succeeded (TrackingEnabled: %v)",
            trackingEnabled);
        if (trackingEnabled) {
            Lease_->Extend(startTime + NProfiling::DurationToCpuDuration(Config_->LeaderLeaseTimeout));
        }
    } else {
        YT_LOG_DEBUG(checkResult, "Leader lease check failed (TrackingEnabled: %v)",
            trackingEnabled);
        if (trackingEnabled) {
            LeaseLost_.Fire(checkResult);
        }
    }
    checkPromise.TrySet(checkResult);
}

TFuture<void> TLeaseTracker::FireLeaseCheck()
{
    VERIFY_THREAD_AFFINITY(ControlThread);

    std::vector<TFuture<void>> futures;
    futures.push_back(New<TFollowerPinger>(this)->Run());
    for (const auto& callback : CustomLeaseCheckers_) {
        futures.push_back(callback());
    }
    return AllSucceeded(futures);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
