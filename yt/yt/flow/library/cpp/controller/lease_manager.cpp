#include "private.h"

#include "chunked_modification.h"
#include "config.h"
#include "lease_manager.h"
#include "yt_connector.h"

#include <yt/yt/flow/library/cpp/common/dyntable_lease.h>
#include <yt/yt/flow/library/cpp/common/flow_view.h>

#include <yt/yt/flow/library/cpp/native_client/public.h>

#include <yt/yt/client/api/transaction.h>

#include <yt/yt/client/tablet_client/public.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/core/concurrency/delayed_executor.h>

#include <yt/yt/core/ypath/helpers.h>

#include <library/cpp/containers/concurrent_hash_set/concurrent_hash_set.h>
#include <library/cpp/iterator/zip.h>
#include <library/cpp/yt/threading/atomic_object.h>
#include <yt/yt/client/api/chaos_client.h>
#include <yt/yt/client/api/prerequisite.h>
#include <yt/yt/core/concurrency/periodic_executor.h>
#include <yt/yt/server/lib/chaos_election/chaos_lease.h>

namespace NYT::NFlow::NController {

using namespace NYT::NApi;
using namespace NYT::NConcurrency;
using namespace NYT::NTransactionClient;

////////////////////////////////////////////////////////////////////////////////

constinit const auto Logger = ControllerLogger;

////////////////////////////////////////////////////////////////////////////////

//! Job leases: the prerequisites workers attach to the commits of their epochs.
//!
//! A lease is created by the leader when a job appears and lives as long as the leader keeps it
//! alive; a worker that lost its lease can no longer commit, which is what makes a job removal safe.
//! Leases outlive leadership: a new leader re-attaches to the ones recorded in the flow view instead
//! of recreating them, so a controller failover does not disturb the running jobs.
//!
//! Two flavours, following the election backend (see EElectionBackend):
//!   - Cypress: a master transaction, kept alive by the client's own pinger;
//!   - Chaos: a chaos lease, which nothing pings on its own — the manager does it. Commits touching
//!     chaos tables accept no other kind of prerequisite.
class TPrerequisiteLeaseManager
    : public ILeaseManager
{
public:
    TPrerequisiteLeaseManager(
        IYTConnectorPtr connector,
        TLeaseManagerConfigPtr config,
        TChaosElectionBackendConfigPtr chaosConfig,
        IInvokerPtr invoker)
        : Connector_(std::move(connector))
        , Config_(std::move(config))
        , ChaosLeaseFactory_(chaosConfig
                ? New<NChaosElection::TChaosLeaseFactory>(Connector_->GetClient(), chaosConfig->ChaosCellBundle)
                : nullptr)
        , PingExecutor_(ChaosLeaseFactory_
                ? New<TPeriodicExecutor>(
                    std::move(invoker),
                    BIND(&TPrerequisiteLeaseManager::PingLeases, MakeWeak(this)),
                    Config_->LeasePingPeriod)
                : nullptr)
    { }

    void Initialize()
    {
        // Pinging runs off the scheduling cycle: a cycle that stalls must not cost the pipeline its
        // jobs. The executor lives on the leader's cancelable invoker and stops with the leadership.
        if (PingExecutor_) {
            PingExecutor_->Start();
        }
    }

    void TerminateStrayLeases(const TFlowViewPtr& flowView) override
    {
        THashSet<TLeaseId> knownLeases;
        auto& layout = flowView->State->ExecutionSpec->Layout;
        for (const auto& [jobId, job] : layout->Jobs) {
            knownLeases.insert(job->LeaseId);
        }

        std::vector<TLeaseId> leasesToTerminate;
        for (const auto& [leaseId, lease] : Leases_) {
            if (knownLeases.contains(leaseId) || ExpiredLeases_.Contains(leaseId)) {
                continue;
            }
            leasesToTerminate.push_back(leaseId);
        }

        std::vector<TLeaseId> terminatingLeases;
        ui64 terminatedLeases = 0;
        std::vector<TFuture<void>> abortFutures;

        auto flush = [&] () {
            WaitFor(AllSucceeded(abortFutures))
                .ThrowOnError();
            abortFutures = {};
            for (auto leaseId : terminatingLeases) {
                UnregisterLease(leaseId);
            }
            terminatedLeases += terminatingLeases.size();
            terminatingLeases = {};
        };

        TPrerequisiteAbortOptions abortOptions;
        // A chaos lease is aborted by removing its object: if it expired just now, the removal must
        // not fail the whole scheduling iteration.
        abortOptions.Force = static_cast<bool>(ChaosLeaseFactory_);

        for (const auto& leaseId : leasesToTerminate) {
            terminatingLeases.push_back(leaseId);
            abortFutures.emplace_back(GetOrCrash(Leases_, leaseId)->Abort(abortOptions));
            if (std::ssize(abortFutures) >= Config_->MaxConcurrentRequests) {
                flush();
            }
        }

        flush();

        YT_TLOG_INFO("Terminated leases")
            .With("LeaseCount", terminatedLeases);
    }

    void CheckLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;
        ui64 attachedLeases = 0;
        ui64 totalLeases = 0;

        std::vector<TJobPtr> jobsToAttach;

        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->LeaseId == NullLeaseId) {
                continue;
            }
            totalLeases += 1;

            if (!Leases_.contains(job->LeaseId)) {
                jobsToAttach.push_back(job);
            }
        }

        attachedLeases = AttachLeases(jobsToAttach);

        std::vector<TJobPtr> expiredLeaseJobs;
        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->LeaseId != NullLeaseId && ExpiredLeases_.Contains(job->LeaseId)) {
                expiredLeaseJobs.push_back(job);
            }
        }

        for (const auto& job : expiredLeaseJobs) {
            layout->RemoveJob(job->JobId, EJobFinishReason::ExpiredLease);
            // Nothing refers to a dead lease anymore: the partition's next job gets a fresh one.
            ForgetLease(job->LeaseId);

            auto partition = GetOrCrash(layout->Partitions, job->PartitionId);
            auto error = TError("Job is lost since its lease has expired")
                .With("job_id", job->JobId)
                .With("partition_id", job->PartitionId)
                .With("computation_id", partition->ComputationId)
                .With("lease_id", job->LeaseId);
            YT_TLOG_EVENT(PublicControllerLogger, NLogging::ELogLevel::Error, "")
                .With(error);

            auto partitionState = flowView->EphemeralState->GetPartitionState(job->PartitionId);
            partitionState->PreviousJobFailInstant = TInstant::Seconds(flowView->State->CurrentTimestamp.Underlying());
            partitionState->PreviousJobFailError = std::move(error);
        }

        YT_TLOG_INFO("Check leases")
            .With("Attached", attachedLeases)
            .With("Expired", expiredLeaseJobs.size())
            .With("Total", totalLeases);
    }

    void PrepareLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        struct TCreateJobLease
        {
            TJobId JobId;
            TLeaseId LeaseId;
        };

        ssize_t createdLeases = 0;

        std::vector<TFuture<TCreateJobLease>> futures;
        auto flush = [&] () {
            auto results = WaitFor(AllSucceeded(futures)).ValueOrThrow();
            for (auto& result : results) {
                layout->UpdateJob(result.JobId, result.LeaseId);
                createdLeases++;
            }
            futures = {};
        };

        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->LeaseId != NullLeaseId) {
                continue;
            }

            auto registerLease = BIND([weakThis = MakeWeak(this), jobId] (const IPrerequisitePtr& lease) {
                if (auto strongThis = weakThis.Lock()) {
                    strongThis->RegisterLease(lease);
                    return TCreateJobLease{jobId, lease->GetId()};
                }
                THROW_ERROR_EXCEPTION("Lease manager is dead");
            });

            futures.push_back(StartLease().Apply(registerLease));
            if (std::ssize(futures) >= Config_->MaxConcurrentRequests) {
                flush();
            }
        }

        flush();
        YT_TLOG_INFO("Prepared leases")
            .With("LeaseCount", createdLeases);
    }

private:
    const IYTConnectorPtr Connector_;
    const TLeaseManagerConfigPtr Config_;
    //! Set for the chaos backend only; null selects master transaction leases.
    const NChaosElection::TChaosLeaseFactoryPtr ChaosLeaseFactory_;
    const TPeriodicExecutorPtr PingExecutor_;

    //! Guards #Leases_ against the ping executor, which runs off the scheduling cycle.
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, LeasesLock_);
    THashMap<TLeaseId, IPrerequisitePtr> Leases_;
    TConcurrentHashSet<TLeaseId> ExpiredLeases_;

    //! Ping outcomes of the round being dispatched, summarized by the next one.
    std::atomic<i64> FailedPingCount_ = 0;
    NThreading::TAtomicObject<TError> LastPingError_;

    TFuture<IPrerequisitePtr> StartLease()
    {
        if (ChaosLeaseFactory_) {
            // The chaos node keeps no attributes of its own, so the lease carries no job title; the
            // job to lease mapping lives in the flow view.
            return ChaosLeaseFactory_->CreateLease(Config_->LeaseTimeout)
                .Apply(BIND([client = Connector_->GetClient()] (NChaosClient::TChaosLeaseId leaseId) {
                    TChaosLeaseAttachOptions options;
                    // The lease has just been created; a confirming ping would only cost a round trip.
                    options.Ping = false;
                    return client->AttachChaosLease(leaseId, options);
                }));
        }

        TTransactionStartOptions options;
        options.Timeout = Config_->LeaseTimeout;
        options.PingPeriod = Config_->LeasePingPeriod;
        auto attributes = NYTree::CreateEphemeralAttributes();
        attributes->Set("title", "Flow: job lease");
        options.Attributes = std::move(attributes);
        return Connector_->GetClient()->StartTransaction(ETransactionType::Master, options).As<IPrerequisitePtr>();
    }

    TFuture<IPrerequisitePtr> AttachLease(TLeaseId leaseId)
    {
        if (ChaosLeaseFactory_) {
            // Ping on attach: a lease that is already gone must be reported right here, since after
            // this point nothing but the periodic ping would notice.
            return Connector_->GetClient()->AttachChaosLease(leaseId, {});
        }

        TTransactionAttachOptions options;
        options.PingPeriod = Config_->LeasePingPeriod;
        return MakeFuture<IPrerequisitePtr>(Connector_->GetClient()->AttachTransaction(leaseId, options));
    }

    //! Attaches to the leases of |jobs| and returns how many attachments succeeded. A lease that
    //! turns out to be gone is recorded as expired, so the next check removes its job.
    ui64 AttachLeases(const std::vector<TJobPtr>& jobs)
    {
        ui64 attachedLeases = 0;

        std::vector<TLeaseId> attachingLeases;
        std::vector<TFuture<IPrerequisitePtr>> futures;

        auto flush = [&] () {
            auto results = WaitFor(AllSet(futures)).ValueOrThrow();
            for (const auto& [leaseId, leaseOrError] : Zip(attachingLeases, results)) {
                if (leaseOrError.IsOK()) {
                    RegisterLease(leaseOrError.Value());
                    attachedLeases += 1;
                } else if (leaseOrError.FindMatching(NYTree::EErrorCode::ResolveError)) {
                    YT_TLOG_WARNING("Job lease is gone")
                        .With("LeaseId", leaseId)
                        .With(leaseOrError);
                    ExpiredLeases_.Insert(leaseId);
                } else {
                    // Transient: the next scheduling iteration retries the attachment.
                    YT_TLOG_WARNING("Failed to attach to a job lease")
                        .With("LeaseId", leaseId)
                        .With(leaseOrError);
                }
            }
            attachingLeases = {};
            futures = {};
        };

        for (const auto& job : jobs) {
            attachingLeases.push_back(job->LeaseId);
            futures.push_back(AttachLease(job->LeaseId));
            if (std::ssize(futures) >= Config_->MaxConcurrentRequests) {
                flush();
            }
        }

        flush();

        return attachedLeases;
    }

    //! Chaos leases have no client-side pinger of their own.
    void PingLeases()
    {
        std::vector<std::pair<TLeaseId, IPrerequisitePtr>> leases;
        {
            auto guard = Guard(LeasesLock_);
            leases.reserve(Leases_.size());
            for (const auto& [leaseId, lease] : Leases_) {
                leases.emplace_back(leaseId, lease);
            }
        }

        // The failures of the previous round, reported here: a ping is fire-and-forget, so the
        // outcome is only known after this round has already been dispatched. Jobs whose leases
        // stop being pinged die, so a silent failure stream must not need a debug log to be seen.
        if (auto failedPings = FailedPingCount_.exchange(0); failedPings > 0) {
            YT_TLOG_INFO("Failed to ping job leases")
                .With("FailedPingCount", failedPings)
                .With("LastError", LastPingError_.Load());
        }

        for (const auto& [leaseId, lease] : leases) {
            if (ExpiredLeases_.Contains(leaseId)) {
                continue;
            }
            // A ping that finds the lease gone fires Aborted by itself, which is what marks the
            // lease expired; every other outcome is transient and retried by the next round.
            lease->Ping().Subscribe(BIND([weakThis = MakeWeak(this), leaseId] (const TError& error) {
                if (error.IsOK()) {
                    return;
                }
                if (auto strongThis = weakThis.Lock()) {
                    strongThis->FailedPingCount_.fetch_add(1);
                    strongThis->LastPingError_.Store(error);
                }
                YT_TLOG_DEBUG("Failed to ping a job lease")
                    .With("LeaseId", leaseId)
                    .With(error);
            }));
        }

        YT_TLOG_DEBUG("Pinged job leases")
            .With("LeaseCount", leases.size());
    }

    void RegisterLease(const IPrerequisitePtr& lease)
    {
        lease->SubscribeAborted(BIND(&TPrerequisiteLeaseManager::AbortLease, MakeWeak(this), lease->GetId()));
        auto guard = Guard(LeasesLock_);
        Leases_[lease->GetId()] = lease;
    }

    void ForgetLease(const TLeaseId& leaseId)
    {
        {
            auto guard = Guard(LeasesLock_);
            Leases_.erase(leaseId);
        }
        ExpiredLeases_.Erase(leaseId);
    }

    void UnregisterLease(const TLeaseId& leaseId)
    {
        {
            auto guard = Guard(LeasesLock_);
            Leases_.erase(leaseId);
        }
        YT_VERIFY(ExpiredLeases_.Erase(leaseId));
    }

    void AbortLease(const TLeaseId& leaseId, const TError& /*error*/)
    {
        ExpiredLeases_.Insert(leaseId);
    }
};

////////////////////////////////////////////////////////////////////////////////

//! Fences jobs by rows of the pipeline's leases dynamic table (see TDyntableLeases); no YT lease
//! transactions are created at all.
class TDyntableLeaseManager
    : public ILeaseManager
{
public:
    TDyntableLeaseManager(
        IYTConnectorPtr connector,
        TLeaseManagerConfigPtr config,
        i64 maxWritesPerTransaction)
        : Connector_(std::move(connector))
        , Config_(std::move(config))
        , MaxWritesPerTransaction_(maxWritesPerTransaction)
        , Leases_(
            NYPath::YPathJoin(Connector_->GetPipelinePath().GetPath(), FlowControlTableName),
            NYPath::YPathJoin(Connector_->GetPipelinePath().GetPath(), LeasesTableName))
    { }

    //! Moves the pipeline-wide deadline when it is due, and nothing else. Every lease of the
    //! pipeline dies at that instant, so this single row is the entire prolongation mechanism:
    //! there is nothing per-partition to prolong and nothing whose cost grows with the pipeline.
    //! The deadline is also rewritten by every grant, so a long grant pass feeds it by itself.
    //!
    //! The lease table is read here exactly once, when this controller takes leadership: that is
    //! the only moment rows can exist that this controller did not write itself. From then on
    //! #GrantedPartitions_ is its own record of them.
    void CheckLeases(const TFlowViewPtr& /*flowView*/) override
    {
        CollectGrantedPartitionsIfNecessary();

        if (TInstant::Now() < NextDeadlineTouchInstant_) {
            return;
        }
        try {
            auto transaction = WaitFor(Connector_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            Leases_.TouchLeaseDeadline(transaction, Config_->LeaseTimeout);
            WaitFor(transaction->Commit())
                .ThrowOnError();
        } catch (const std::exception& ex) {
            // Not fatal for the iteration: the deadline still has most of its ttl left (the
            // touches are spaced at a fraction of it), and every following iteration retries.
            YT_TLOG_WARNING("Failed to touch the dyntable lease deadline")
                .With(TError(ex));
            return;
        }
        OnDeadlineTouched();
        YT_TLOG_INFO("Touched the dyntable lease deadline")
            .With("LeaseTimeout", Config_->LeaseTimeout);
    }

    //! Installs a lease for every job that has none yet, then marks the jobs. The rows commit
    //! first: if the controller dies in between, the next iteration re-grants the same values
    //! idempotently. Workers only see the job flag after the layout mutation commits, i.e. after
    //! the rows are in place.
    //!
    //! Two kinds of job are handled differently, and #GrantedPartitions_ is what tells them apart.
    //! A job on a partition that holds no lease has no incumbent to shut out, so both its rows go
    //! in one transaction. A job that takes a partition over from another one does have an
    //! incumbent, still committing under the old lease, so it goes through the same two phases as
    //! a revocation — otherwise the single write races that worker on the "existence" row and can
    //! lose indefinitely, taking the whole chunk with it.
    void PrepareLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        std::vector<TJobPtr> freshJobs;
        std::vector<TJobPtr> movingJobs;
        for (const auto& [jobId, job] : layout->Jobs) {
            if (job->DyntableLease || job->LeaseId != NullLeaseId) {
                continue;
            }
            if (GrantedPartitions_.contains(job->PartitionId)) {
                movingJobs.push_back(job);
            } else {
                freshJobs.push_back(job);
            }
        }
        if (freshJobs.empty() && movingJobs.empty()) {
            return;
        }

        ssize_t grantedJobs = 0;
        std::vector<TError> failures;

        // Marking a job is what publishes it, so it happens only once the row that completes its
        // lease has committed — for a fresh job that is the single grant, for a moving one it is
        // phase 2.
        auto publish = [&] (const std::vector<TJobPtr>& chunk) {
            OnDeadlineTouched();
            for (const auto& job : chunk) {
                auto newJob = CloneYsonStruct(job);
                newJob->DyntableLease = true;
                layout->UpdateJob(newJob);
                // Recorded when the rows commit, not when the iteration does: rows written by an
                // iteration that is later discarded are still out there, and this is what
                // remembers them so that a later sweep can find them.
                GrantedPartitions_.insert(job->PartitionId);
                ++grantedJobs;
            }
        };

        // A fresh grant writes two rows per job, plus the deadline row shared by the chunk. It
        // cannot conflict with anybody, so there is nothing to split.
        auto freshFailures = ModifyInChunks<TJobPtr>(
            "grant",
            freshJobs,
            std::max<ssize_t>(1, (MaxWritesPerTransaction_ - 1) / 2),
            /*splitOnConflict*/ false,
            [this] (const ITransactionPtr& transaction, const std::vector<TJobPtr>& chunk) {
                for (const auto& job : chunk) {
                    Leases_.GrantPartitionLease(transaction, job->PartitionId, job->JobId);
                }
                Leases_.TouchLeaseDeadline(transaction, Config_->LeaseTimeout);
            },
            publish);
        failures.insert(failures.end(), freshFailures.begin(), freshFailures.end());

        // Phase 1 shuts the incumbents out. One row per job, and workers never write that row.
        auto phase1Failures = ModifyInChunks<TJobPtr>(
            "grant_phase1",
            movingJobs,
            MaxWritesPerTransaction_,
            /*splitOnConflict*/ false,
            [this] (const ITransactionPtr& transaction, const std::vector<TJobPtr>& chunk) {
                for (const auto& job : chunk) {
                    Leases_.GrantPartitionLeasePhase1(transaction, job->PartitionId, job->JobId);
                }
            });
        failures.insert(failures.end(), phase1Failures.begin(), phase1Failures.end());

        // Phase 2 completes the handover. It can conflict with whatever the incumbent still had
        // in flight, but only once per partition, so halving isolates the guilty rows.
        if (phase1Failures.empty()) {
            auto phase2Failures = ModifyInChunks<TJobPtr>(
                "grant_phase2",
                movingJobs,
                std::max<ssize_t>(1, MaxWritesPerTransaction_ - 1),
                /*splitOnConflict*/ true,
                [this] (const ITransactionPtr& transaction, const std::vector<TJobPtr>& chunk) {
                    for (const auto& job : chunk) {
                        Leases_.GrantPartitionLeasePhase2(transaction, job->PartitionId, job->JobId);
                    }
                    Leases_.TouchLeaseDeadline(transaction, Config_->LeaseTimeout);
                },
                publish);
            failures.insert(failures.end(), phase2Failures.begin(), phase2Failures.end());
        }

        YT_TLOG_INFO("Granted dyntable leases")
            .With("LeaseCount", grantedJobs)
            .With("FreshJobs", std::ssize(freshJobs))
            .With("MovingJobs", std::ssize(movingJobs))
            .With("FailedChunks", std::ssize(failures));

        // The jobs of a chunk that never committed are still in the layout unmarked, and a job
        // published without a lease would run its partition with nothing fencing it. Failing here
        // discards the whole iteration, including the jobs themselves, and the next one creates
        // them again and grants their leases from scratch.
        if (!failures.empty()) {
            THROW_ERROR_EXCEPTION("Failed to grant dyntable leases for %v of %v jobs",
                std::ssize(freshJobs) + std::ssize(movingJobs) - grantedJobs,
                std::ssize(freshJobs) + std::ssize(movingJobs))
                .With(std::move(failures));
        }
    }

    //! Revokes every lease this controller handed out to a partition that no longer has a job.
    //!
    //! Same shape as the transaction backend's #TerminateStrayLeases: the manager's own record of
    //! what it handed out, minus what the live layout still accounts for. The cost is the size of
    //! that difference — a handful of partitions in steady state, and nothing at all once a paused
    //! pipeline has been swept — rather than the size of the pipeline.
    //!
    //! A partition that still has a job is left alone, including one whose grant is pending: the
    //! grant overwrites both rows, which fences the previous owner in a single write.
    void TerminateStrayLeases(const TFlowViewPtr& flowView) override
    {
        const auto& layout = flowView->State->ExecutionSpec->Layout;

        THashSet<TPartitionId> assignedPartitions;
        assignedPartitions.reserve(layout->Jobs.size());
        for (const auto& [jobId, job] : layout->Jobs) {
            assignedPartitions.insert(job->PartitionId);
        }

        std::vector<TPartitionId> stray;
        for (const auto& partitionId : GrantedPartitions_) {
            if (!assignedPartitions.contains(partitionId)) {
                stray.push_back(partitionId);
            }
        }
        if (stray.empty()) {
            return;
        }

        RevokeInChunks(stray);
        for (const auto& partitionId : stray) {
            GrantedPartitions_.erase(partitionId);
        }

        YT_TLOG_INFO("Revoked stray dyntable leases")
            .With("PartitionCount", std::ssize(stray))
            .With("GrantedPartitions", std::ssize(GrantedPartitions_));
    }

private:
    const IYTConnectorPtr Connector_;
    const TLeaseManagerConfigPtr Config_;
    const i64 MaxWritesPerTransaction_;
    const TDyntableLeases Leases_;

    //! The partitions this controller has written lease rows for and not yet revoked — its own
    //! record of the table, the counterpart of the lease transactions the transaction backend
    //! keeps in #Leases_. Seeded from the table once when leadership is taken, maintained by the
    //! grants and revocations from then on. Only the scheduling fiber touches it, and its calls
    //! are strictly sequential, so no synchronization is needed.
    THashSet<TPartitionId> GrantedPartitions_;

    //! Whether #GrantedPartitions_ has been collected from the table. A leader object is created
    //! per leadership, so this happens exactly once per leadership and never again.
    bool CollectedGrantedPartitions_ = false;

    //! When the pipeline-wide deadline is due for a rewrite. A plain local instant on purpose:
    //! it measures an elapsed interval rather than comparing against the cluster clock, so a
    //! skewed local clock cannot make the touches late. Zero until the first one, which is what
    //! makes a fresh leader write the deadline on its very first iteration.
    TInstant NextDeadlineTouchInstant_;

    void OnDeadlineTouched()
    {
        NextDeadlineTouchInstant_ = TInstant::Now() + Config_->LeaseTimeout / DeadlineTouchesPerTimeout;
    }

    //! How many times the deadline is rewritten within one lease timeout. Three leaves two whole
    //! retry windows between a failing touch and an expired fleet.
    static constexpr int DeadlineTouchesPerTimeout = 3;

    //! The one and only read of the lease table, at the start of the leadership. Whatever the
    //! predecessor knew died with it, so this is the only way to learn which partitions its rows
    //! name; from then on the record is maintained in memory and the table is never scanned again.
    //! Nothing is revoked here: the partitions among these that no job accounts for are strays
    //! like any other, and #TerminateStrayLeases sweeps them later in this very iteration.
    void CollectGrantedPartitionsIfNecessary()
    {
        if (CollectedGrantedPartitions_) {
            return;
        }

        auto scannedPartitionIds = WaitFor(Leases_.ListPartitionLeases(Connector_->GetClient()))
            .ValueOrThrow();
        GrantedPartitions_ = THashSet<TPartitionId>(scannedPartitionIds.begin(), scannedPartitionIds.end());

        CollectedGrantedPartitions_ = true;

        YT_TLOG_INFO("Read the dyntable leases left by the previous leadership")
            .With("PartitionCount", std::ssize(GrantedPartitions_));
    }

    //! Runs both revocation phases over |partitionIds|.
    void RevokeInChunks(const std::vector<TPartitionId>& partitionIds)
    {
        // Phase 1: forbid the (possibly dead) workers to start new transactions. Workers never
        // write the expiration rows, so this phase cannot conflict with them at all.
        ThrowOnRevocationFailures("revoke_phase1", ModifyInChunks<TPartitionId>("revoke_phase1", partitionIds, MaxWritesPerTransaction_,
            /*splitOnConflict*/ false,
            [this] (const ITransactionPtr& transaction, const std::vector<TPartitionId>& chunk) {
                for (const auto& partitionId : chunk) {
                    Leases_.RevokePartitionLeasePhase1(transaction, partitionId);
                }
            }));

        // Phase 2: forbid committing the in-flight worker transactions. Each partition can
        // conflict at most once: after phase 1 its worker cannot start another transaction.
        ThrowOnRevocationFailures("revoke_phase2", ModifyInChunks<TPartitionId>("revoke_phase2", partitionIds, MaxWritesPerTransaction_,
            /*splitOnConflict*/ true,
            [this] (const ITransactionPtr& transaction, const std::vector<TPartitionId>& chunk) {
                for (const auto& partitionId : chunk) {
                    Leases_.RevokePartitionLeasePhase2(transaction, partitionId);
                }
            }));
    }

    //! A revocation that does not land must not be papered over: its partition has no job in the
    //! layout while its lease is still live, and persisting that layout is what would let a second
    //! job of the same partition start next to the first one. Throwing discards the iteration
    //! together with the job removals that made these partitions stray.
    void ThrowOnRevocationFailures(TStringBuf phase, const std::vector<TError>& failures)
    {
        if (!failures.empty()) {
            THROW_ERROR_EXCEPTION("Failed to revoke dyntable leases in %v chunks", std::ssize(failures))
                .With("phase", phase)
                .With(failures);
        }
    }

    //! Applies |modify| to |items| in transactions of at most |itemsPerChunk| items each; see
    //! #NController::ModifyInChunks for the chunking, the retry policy and what |splitOnConflict|
    //! claims about the conflicts of a phase.
    //!
    //! The chunks are committed one after another, each in its own transaction fenced by the
    //! leader row. Committing them in parallel would mean not touching that row — every chunk
    //! writes it, so they would conflict with each other — and the sets are small enough that the
    //! stronger fence is worth more than the concurrency.
    template <class T>
    std::vector<TError> ModifyInChunks(
        TStringBuf phase,
        const std::vector<T>& items,
        ssize_t itemsPerChunk,
        bool splitOnConflict,
        const std::function<void(const ITransactionPtr&, const std::vector<T>&)>& modify,
        const std::function<void(const std::vector<T>&)>& onCommitted = {})
    {
        return NController::ModifyInChunks<T>(
            phase,
            items,
            itemsPerChunk,
            splitOnConflict,
            [&] (const std::vector<T>& chunk) {
                return CommitChunk(chunk, modify);
            },
            onCommitted);
    }

    template <class T>
    TError CommitChunk(
        const std::vector<T>& chunk,
        const std::function<void(const ITransactionPtr&, const std::vector<T>&)>& modify)
    {
        try {
            auto transaction = WaitFor(Connector_->StartTransaction(ETransactionType::Tablet))
                .ValueOrThrow();
            modify(transaction, chunk);
            WaitFor(transaction->Commit())
                .ThrowOnError();
            return {};
        } catch (const std::exception& ex) {
            return TError(ex);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

ILeaseManagerPtr CreateLeaseManager(
    IYTConnectorPtr connector,
    TLeaseManagerConfigPtr config,
    EElectionBackend backend,
    TChaosElectionBackendConfigPtr chaosConfig,
    i64 maxWritesPerTransaction,
    IInvokerPtr invoker)
{
    switch (backend) {
        case EElectionBackend::Dyntable:
            return New<TDyntableLeaseManager>(
                std::move(connector),
                std::move(config),
                maxWritesPerTransaction);

        case EElectionBackend::Cypress:
        case EElectionBackend::Chaos: {
            if (backend != EElectionBackend::Chaos) {
                chaosConfig = nullptr;
            } else {
                YT_VERIFY(chaosConfig);
            }
            auto leaseManager = New<TPrerequisiteLeaseManager>(
                std::move(connector),
                std::move(config),
                std::move(chaosConfig),
                std::move(invoker));
            leaseManager->Initialize();
            return leaseManager;
        }
    }
    YT_ABORT();
}

} // namespace NYT::NFlow::NController
