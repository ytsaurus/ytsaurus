#include "dyntable_election_manager.h"

#include "private.h"

#include <yt/yt/client/api/client.h>

#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/core/concurrency/scheduler.h>
#include <yt/yt/core/concurrency/thread_affinity.h>

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NFlow::NController {

using namespace NConcurrency;
using namespace NApi;
using namespace NLockElection;
using namespace NPrerequisiteClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

class TDyntableElectionManager
    : public IDyntableElectionManager
{
public:
    TDyntableElectionManager(
        IClientPtr client,
        IInvokerPtr invoker,
        TDyntableElectionManagerOptions options)
        : Client_(std::move(client))
        , Invoker_(std::move(invoker))
        , Options_(std::move(options))
        , Leases_(Options_.FlowControlTablePath, Options_.LeasesTablePath)
    {
        YT_VERIFY(Invoker_->IsSerialized());
    }

    void Start() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        bool expected = false;
        if (!Active_.compare_exchange_strong(expected, true)) {
            return;
        }

        PeriodicExecutor_ = New<TPeriodicExecutor>(
            Invoker_,
            BIND(&TDyntableElectionManager::Iteration, MakeWeak(this)),
            Options_.CapturePeriod);
        PeriodicExecutor_->Start();
        YT_TLOG_INFO("Dyntable election manager started")
            .With("LeasesTablePath", Options_.LeasesTablePath)
            .With("LeaseTtl", Options_.LeaseTtl)
            .With("DetachTimeout", Options_.DetachTimeout);
    }

    TFuture<void> Stop() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        bool expected = true;
        if (!Active_.compare_exchange_strong(expected, false)) {
            return MakeFuture(TError());
        }

        auto executor = PeriodicExecutor_;
        return BIND([this, this_ = MakeStrong(this), executor] {
            Demote("Election manager is stopping");
            if (executor) {
                WaitUntilSet(executor->Stop());
            }
        })
            .AsyncVia(Invoker_)
            .Run();
    }

    bool IsActive() const override
    {
        return Active_.load();
    }

    TFuture<void> StopLeading() override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return BIND(&TDyntableElectionManager::Demote, MakeStrong(this), std::string("Leadership stop requested"))
            .AsyncVia(Invoker_)
            .Run();
    }

    TPrerequisiteId GetPrerequisiteId() const override
    {
        // There is no prerequisite in the dyntable protocol: fencing happens inside every
        // transaction via the leader row.
        return NObjectClient::NullObjectId;
    }

    bool IsLeader() const override
    {
        return Leading_.load();
    }

    void SetRecoveryRenewalEnabled(bool enabled, ui64 leadershipEpoch) override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        // The epoch check and the switch are one operation: reading the epoch and flipping the
        // flag separately lets a callback that passed the check pause, lose its leadership to a
        // re-acquisition that arms the renewal for the next epoch, and then disarm it on resume.
        ui64 currentEpoch;
        bool changed = false;
        {
            auto guard = Guard(RecoveryRenewalLock_);
            currentEpoch = LeadershipEpoch_.load();
            if (leadershipEpoch == currentEpoch) {
                changed = RecoveryRenewalEnabled_.exchange(enabled) != enabled;
            }
        }

        if (leadershipEpoch != currentEpoch) {
            YT_TLOG_INFO("Dyntable election: ignored a recovery renewal switch from a foreign leadership")
                .With("Enabled", enabled)
                .With("CallerEpoch", leadershipEpoch)
                .With("CurrentEpoch", currentEpoch);
        } else if (changed) {
            YT_TLOG_INFO("Dyntable election: recovery renewal switched")
                .With("Enabled", enabled)
                .With("LeadershipEpoch", leadershipEpoch);
        }
    }

    ui64 GetLeadershipEpoch() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return LeadershipEpoch_.load();
    }

    bool IsRecoveryRenewalEnabled() const override
    {
        YT_ASSERT_THREAD_AFFINITY_ANY();

        return RecoveryRenewalEnabled_.load();
    }

    DEFINE_SIGNAL_OVERRIDE(void(), LeadingStarted);
    DEFINE_SIGNAL_OVERRIDE(void(), LeadingEnded);

private:
    const IClientPtr Client_;
    const IInvokerPtr Invoker_;
    const TDyntableElectionManagerOptions Options_;
    const TDyntableLeases Leases_;

    std::atomic<bool> Active_ = false;
    std::atomic<bool> Leading_ = false;
    //! Armed on every leadership acquisition; the controller disarms it once the first
    //! scheduling iteration commits and the fenced transactions take over feeding the lease.
    std::atomic<bool> RecoveryRenewalEnabled_ = false;
    //! Identifies the current leadership; see #SetRecoveryRenewalEnabled.
    std::atomic<ui64> LeadershipEpoch_ = 0;
    //! Guards the pair (#LeadershipEpoch_, #RecoveryRenewalEnabled_) so that a stale callback
    //! cannot disarm the renewal of a leadership acquired in between; reads stay lock-free.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, RecoveryRenewalLock_);
    TInstant LastRenewalSuccess_;

    TPeriodicExecutorPtr PeriodicExecutor_;

    void Iteration()
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (!Active_.load()) {
            return;
        }

        // The current leader must not recapture its own expired lease outright: an expired lease
        // means the work cycle stopped prolonging it, and the proper reaction is to demote (and
        // possibly re-elect from scratch on a later iteration, racing the replicas fairly).
        auto result = Leases_.TryCaptureLeader(
            Client_,
            Options_.IncarnationId,
            Options_.Address,
            Options_.LeaseTtl,
            /*captureAllowed*/ !Leading_.load(),
            /*renewAllowed*/ Leading_.load() && RecoveryRenewalEnabled_.load());
        auto now = TInstant::Now();

        if (result.IsLeader) {
            LastRenewalSuccess_ = now;
            if (!Leading_.exchange(true)) {
                // Recovery starts now: until the controller reports the first committed
                // scheduling iteration, the election loop keeps the lease alive itself. Advancing
                // the epoch and arming the renewal happen under the same lock the switch takes,
                // so a callback of the previous leadership either lands before the pair changes
                // or sees the new epoch and is ignored.
                ui64 epoch;
                {
                    auto guard = Guard(RecoveryRenewalLock_);
                    epoch = LeadershipEpoch_.fetch_add(1) + 1;
                    RecoveryRenewalEnabled_.store(true);
                }
                YT_TLOG_INFO("Dyntable election: recovery renewal switched")
                    .With("Enabled", true)
                    .With("LeadershipEpoch", epoch);
                YT_TLOG_INFO("Dyntable election: leadership acquired")
                    .With("Address", Options_.Address)
                    .With("IncarnationId", Options_.IncarnationId);
                LeadingStarted_.Fire();
            } else if (result.Renewed) {
                YT_TLOG_INFO("Dyntable election: leader lease renewed during recovery");
            }
            return;
        }

        if (result.Error.IsOK()) {
            // A live foreign leader holds the lease, or our own lease expired unprolonged.
            if (Leading_.load()) {
                bool own = result.CurrentLeader && result.CurrentLeader->IncarnationId == Options_.IncarnationId;
                if (own) {
                    Demote("Leader lease expired without prolongation");
                } else {
                    Demote(Format("Leadership captured by %v",
                        result.CurrentLeader ? result.CurrentLeader->Address : "unknown"));
                }
            }
            return;
        }

        YT_TLOG_WARNING("Dyntable election: capture/renew attempt failed")
            .With(result.Error);
        if (Leading_.load() && now - LastRenewalSuccess_ > Options_.DetachTimeout) {
            Demote(Format("No successful lease renewal for %v", now - LastRenewalSuccess_));
        }
    }

    void Demote(const std::string& reason)
    {
        YT_ASSERT_SERIALIZED_INVOKER_AFFINITY(Invoker_);

        if (!Leading_.exchange(false)) {
            return;
        }
        YT_TLOG_INFO("Dyntable election: leadership lost")
            .With("Reason", reason);
        LeadingEnded_.Fire();
    }
};

////////////////////////////////////////////////////////////////////////////////

IDyntableElectionManagerPtr CreateDyntableElectionManager(
    IClientPtr client,
    IInvokerPtr invoker,
    TDyntableElectionManagerOptions options)
{
    return New<TDyntableElectionManager>(std::move(client), std::move(invoker), std::move(options));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NFlow::NController
